namespace StreamOfDreams


module Json =
    open Newtonsoft.Json
    let inline serialize o =
        JsonConvert.SerializeObject o
    let inline deserialize<'a> o =
        JsonConvert.DeserializeObject<'a> o

module DomainTypes =
    open System
    type EventId = Guid
    type StreamName = string

    type Version =
    | EmptyStream
    | NoStream
    | StreamExists
    | Any
    | Value of uint64

    type SubscriptionPosition =
    /// Always subscribe from the begining.  Same as `Value 0`. Only use if you want to always start from the beginning.
    | Beginning
    /// Use what was persisted last in the database.  This useful for processors that want to start from where they left off.
    | Continue
    /// Use a discrete value.  Useful for if you know exactly where you want to start.  Should not be used for long running processes.
    | Value of uint64

module DbHelpers =
    open System
    open Npgsql
    open NpgsqlTypes
    open Hopac
    open System.Threading

    let inline builderToConnection (connStr : NpgsqlConnectionStringBuilder) =
        new NpgsqlConnection (connStr |> string)

    let inline ensureOpen (connection : NpgsqlConnection) =
        if not <| connection.FullState.HasFlag System.Data.ConnectionState.Open then
            Alt.fromUnitTask (connection.OpenAsync)
        else
            Alt.unit()


    // let inline createAndOpenConnection (connStr : NpgsqlConnectionStringBuilder) =
    //     let conn =
    //         connStr
    //         |> builderToConnection
    //     conn
    //     |> Alt.always
    //     |> Alt.afterJob ensureOpen
    //     |> Alt.afterFun(fun _ -> conn)

    let inline executeReader (cmd : NpgsqlCommand) =
        Alt.fromTask cmd.ExecuteReaderAsync
        |> Job.map (unbox<NpgsqlDataReader>)

    let inline executeReaderCt (external: CancellationToken) (cmd : NpgsqlCommand) =
        Alt.fromTask (fun ct ->
            use cts = CancellationTokenSource.CreateLinkedTokenSource(ct,external)
            cmd.ExecuteReaderAsync cts.Token)
        |> Job.map (unbox<NpgsqlDataReader>)

    let inline executeScalar<'a> (cmd : NpgsqlCommand) =
        Alt.fromTask cmd.ExecuteScalarAsync
        |> Alt.afterFun (unbox<'a>)

    let inline executeNonQuery (cmd : NpgsqlCommand) =
        Alt.fromTask cmd.ExecuteNonQueryAsync

    let inline executeNonQueryIgnore (cmd : NpgsqlCommand) =
        Alt.fromTask cmd.ExecuteNonQueryAsync
        |> Alt.afterFun ignore

    let inline valueOrDbNull opt =
        match opt with
        | Some o -> o |> box
        | None -> DBNull.Value |> box


    let inline addParameter (cmd : NpgsqlCommand) parameter =
        parameter |> cmd.Parameters.Add |> ignore

    let inline inferredParam param (value : obj) =
        NpgsqlParameter(param, value = value)

    let inline explicitParam (dbType : NpgsqlDbType) param value =
        NpgsqlParameter(param, dbType, Value = value)

    let inline jsonBParam param value =
        explicitParam NpgsqlDbType.Jsonb param value

    let inline uuidParam param value =
        explicitParam NpgsqlDbType.Uuid param value
    let inline textParam param value =
        explicitParam NpgsqlDbType.Text param value
    let inline timestampParam param value =
        explicitParam NpgsqlDbType.Timestamp param value
    let inline bigintParam param value =
        explicitParam NpgsqlDbType.Bigint param value

    let inline readRow (reader: NpgsqlDataReader) =
        let readValueAsync fieldIndex =
          job {
              let fieldName = reader.GetName fieldIndex
              let! isNull = reader.IsDBNullAsync fieldIndex
              if isNull then
                return fieldName, None
              else
                let! value = reader.GetFieldValueAsync fieldIndex
                return fieldName, Some value
          }

        [0 .. reader.FieldCount - 1]
        |> List.map readValueAsync
        |> Job.seqCollect
        |> Job.map List.ofSeq

    let inline readTable (reader: NpgsqlDataReader) =
        let rec readRows rows = job {
            let! canRead = reader.ReadAsync()
            if canRead then
              let! row = readRow reader
              return! readRows (row :: rows)
            else
              return rows
        }
        readRows []

    let inline readFirstRow  (reader: NpgsqlDataReader) =
        reader
        |> readTable
        |> Job.map Seq.tryHead

    let inline mapRow f =
        readTable
        >> Job.map (List.choose f)

module DbTypes =
    open System
    open DomainTypes

    /// Record for the stream table
    type Stream = {
        Id : int64
        Name : StreamName
        Version : uint64
        CreatedAt : DateTime
    }
    /// A new event that has yet to be written
    type Event = {
        EventType : string
        CorrelationId: Guid option
        CausationId: Guid option
        Data : string
        Metadata : string option
    }
        with
            static member CreateSimple eventType data =
                {
                    EventType = eventType
                    CorrelationId = None
                    CausationId = None
                    Data = Json.serialize data
                    Metadata = None
                }
            static member AutoTypeData data =
                {
                    EventType = TypeInfo.typeNameToCamelCase data
                    CorrelationId = None
                    CausationId = None
                    Data = Json.serialize data
                    Metadata = None
                }
            static member AutoTypeDataAndMeta data meta =
                {
                    EventType = TypeInfo.typeNameToCamelCase data
                    CorrelationId = None
                    CausationId = None
                    Data = Json.serialize data
                    Metadata = Json.serialize meta |> Some
                }

    /// An event that has been written and is immutable
    type RecordedEvent = {
        Id : Guid
        Number : uint64
        StreamName : StreamName
        StreamVersion : uint64
        CorrelationId: Guid option
        CausationId: Guid option
        EventType: string
        Data: string
        Metadata: string option
        CreatedAt: DateTime
    }
        with
            static member Empty =
                {
                    Id = Guid.Empty
                    Number = 0UL
                    StreamName = String.Empty
                    StreamVersion =0UL
                    CorrelationId = None
                    CausationId= None
                    EventType = String.Empty
                    Data = String.Empty
                    Metadata  = None
                    CreatedAt =  DateTime.MinValue
                }

module Repository =
    open System
    open Npgsql
    open DbTypes
    open DbHelpers
    open Hopac
    open DomainTypes

    let prepareCreateStream conn (streamName : StreamName) =
        let cmd = new NpgsqlCommand(Connection = conn)
        cmd.CommandText <-
            """
            INSERT INTO streams (stream_name)
            VALUES (@streamName)
            ON CONFLICT DO NOTHING;
            SELECT stream_id FROM streams WHERE stream_name = @streamName;

            """
        streamName |> textParam "streamName" |> addParameter cmd
        cmd

    let preparestreamIdAndVersion conn (streamName : StreamName) =
        let cmd = new NpgsqlCommand(Connection = conn)
        cmd.CommandText <-
            """
            SELECT stream_id, stream_version, created_at
            FROM streams
            WHERE stream_name = @streamName;
            """

        streamName |> textParam "streamName" |> addParameter cmd
        cmd


    let prepareCreateStreamEvents conn streamId eventsSaved =
        let eventCount = eventsSaved |> Seq.length
        let cmd = new NpgsqlCommand(Connection = conn)
        let extraParams =
            eventsSaved
            |> Seq.mapi ^ fun index uuid ->
                let pms =
                    [
                        index |> box, bigintParam
                        uuid |> box, uuidParam
                    ]
                let offset = index * Seq.length pms
                let parmeterizedString =
                    pms
                    |> Seq.mapi ^ fun index (value, paramCtor) ->
                        let paramName = offset + index |> string
                        value |> paramCtor paramName |> addParameter cmd //SIDE EFFECT!!!
                        paramName |> String.format "@{0}"
                    |> String.join ","
                    |> String.format "({0})"
                parmeterizedString
            |> String.join ","

        cmd.CommandText <-
          extraParams
          |> String.format
              """
              WITH
                stream AS (
                  UPDATE streams SET stream_version = stream_version + @eventCount
                  WHERE stream_id = @streamId
                  RETURNING stream_version - @eventCount as initial_stream_version, stream_version AS next_expected_version
                ),
                events (index, event_id) AS (
                  VALUES {0}
                ),
                ignoring as (
                  INSERT INTO stream_events
                    (
                      event_id,
                      stream_id,
                      stream_version,
                      original_stream_id,
                      original_stream_version
                    )
                      SELECT
                        events.event_id,
                        @streamId,
                        stream.initial_stream_version + events.index,
                        @streamId,
                        stream.initial_stream_version + events.index
                      FROM events, stream
                )

              SELECT stream.next_expected_version FROM stream;
              """

        streamId |> inferredParam "streamId" |> addParameter cmd
        eventCount |> inferredParam "eventCount" |> addParameter cmd
        cmd


    let prepareLinkEvents conn streamId eventsSaved =
        let eventCount = eventsSaved |> Seq.length
        let cmd = new NpgsqlCommand(Connection = conn)
        let extraParams =
            eventsSaved
            |> Seq.mapi ^ fun index uuid ->
                let pms =
                    [
                        index |> box, bigintParam
                        uuid |> box, uuidParam
                    ]
                let offset = index * Seq.length pms
                let parmeterizedString =
                    pms
                    |> Seq.mapi ^ fun index (value, paramCtor) ->
                        let paramName = offset + index |> string
                        value |> paramCtor paramName |> addParameter cmd //SIDE EFFECT!!!
                        paramName |> String.format "@{0}"
                    |> String.join ","
                    |> String.format "({0})"
                parmeterizedString
            |> String.join ","

        cmd.CommandText <-
          extraParams
          |> String.format
                     """
                      WITH
                        stream AS (
                          UPDATE streams SET stream_version = stream_version + @eventCount
                          WHERE stream_id = @streamId
                          RETURNING stream_version - @eventCount as initial_stream_version, stream_version AS next_expected_version
                        ),
                        events (index, event_id) AS (
                          VALUES {0}
                        ),
                        ignoring as (
                          INSERT INTO stream_events
                            (
                              stream_id,
                              stream_version,
                              event_id,
                              original_stream_id,
                              original_stream_version
                            )
                          SELECT
                            @streamId,
                            stream.initial_stream_version + events.index,
                            events.event_id,
                            original_stream_events.original_stream_id,
                            original_stream_events.stream_version
                          FROM events
                          CROSS JOIN stream
                          INNER JOIN stream_events as original_stream_events
                            ON original_stream_events.event_id = events.event_id
                              AND original_stream_events.stream_id = original_stream_events.original_stream_id
                        )

                      SELECT stream.next_expected_version FROM stream;
                      """

        streamId |> inferredParam "streamId" |> addParameter cmd
        eventCount |> inferredParam "eventCount" |> addParameter cmd
        cmd


    let getStreamByName conn (streamName : StreamName) = job {
        use cmd = preparestreamIdAndVersion conn streamName
        use! reader = cmd |> executeReader

        let! rowOpt = readFirstRow (reader)

        let retVal =
            match rowOpt with
            | None -> None
            | Some row ->
                row
                |> function
                    | [ "stream_id", Some id
                        "stream_version", Some ver
                        "created_at", Some date
                        ] ->
                            Some <| {
                            Id = id |> unbox<int64>
                            Name = streamName
                            Version = ver |> unbox<int64> |> uint64
                            CreatedAt = date |> unbox<DateTime>
                        }
                    | _ -> None

        return retVal
    }

    //TODO: figureOutLimit
    let prepareReadFoward conn streamId (version : uint64) (limit : uint64)=
        let cmd = new NpgsqlCommand(Connection = conn)
        cmd.CommandText <-
            """
            SELECT
              se.stream_version,
              e.event_id,
              s.stream_name,
              se.original_stream_version,
              e.event_type,
              e.correlation_id,
              e.causation_id,
              e.data,
              e.metadata,
              e.created_at
            FROM stream_events se
            INNER JOIN streams s ON s.stream_id = se.original_stream_id
            INNER JOIN events e ON se.event_id = e.event_id
            WHERE se.stream_id = @streamId and se.stream_version >= @version
            ORDER BY se.stream_version
            LIMIT @limit
            """

        streamId |> inferredParam "streamId" |> addParameter cmd
        version |> int64 |> inferredParam "version" |> addParameter cmd
        limit |> int64 |> inferredParam "limit" |> addParameter cmd
        cmd

    let readEventsForwardInner (forever : bool) token (connStr : NpgsqlConnectionStringBuilder) (streamName : StreamName) (startVersion : uint64) (limit : uint64) = job {
        // let limit = 3000

        let getBatch streamId version = job {
            use conn = connStr |> builderToConnection
            do! conn |> ensureOpen
            use cmd = prepareReadFoward conn streamId version limit
            use! reader = cmd |> executeReaderCt token
            let! results =
                reader
                |> mapRow ^ function
                    | [ "stream_version", Some streamVersion
                        "event_id", Some eventId
                        "stream_name", Some streamName
                        "original_stream_version", Some originalVersion
                        "event_type", Some eventType
                        "correlation_id", correlation
                        "causation_id", causatoin
                        "data", Some data
                        "metadata", metadata
                        "created_at", Some createdAt ]
                            -> Some <|
                                {
                                    Id =  eventId |> unbox<Guid>
                                    Number = originalVersion |> unbox<int64> |> uint64
                                    StreamName =  streamName |> unbox<string>
                                    StreamVersion = streamVersion |> unbox<int64> |> uint64
                                    CorrelationId= correlation |> unbox<Guid option>
                                    CausationId = causatoin |> unbox<Guid option>
                                    EventType=  eventType |> unbox<string>
                                    Data = data |> unbox<string>
                                    Metadata=  metadata |> unbox<string option>
                                    CreatedAt = createdAt |> unbox<DateTime>

                                }
                    | _ ->
                        None
            return results
        }
        use conn = connStr |> builderToConnection
        do! conn |> ensureOpen

        let! streamOpt = getStreamByName conn streamName

        match streamOpt with
        | Some s ->
            let xs =
                Stream.unfoldJob ^ fun (start : uint64) ->
                    job {
                        if token.IsCancellationRequested then
                            return None
                        else
                            let! retval = getBatch s.Id start
                            let returnedLength = retval |> Seq.length
                            if returnedLength = 0 then
                                if forever then
                                    do! timeOutMillis 25
                                    return Some ([], (start))
                                else
                                    return None
                            else
                                return Some (retval, (start + uint64 returnedLength))
                    }

                // >> Stream.doFinalizeFun (fun _ -> token.)
            return Some (xs startVersion |> Stream.appendMap (Stream.ofSeq))
        | None ->
            return None
    }

    let readEventsForwardForever = readEventsForwardInner true

    let readEventsForward = readEventsForwardInner false



    /// Max events: 65535/5 = 13107
    let prepareInsertEvents conn (events : DbTypes.RecordedEvent seq) =
        let cmd = new NpgsqlCommand(Connection = conn)

        let buildParameters () =
            events
            |> Seq.mapi ^ fun index item ->

                let pms =
                    [
                        // item.Id |> box , uuidParam
                        item.EventType |> box, textParam
                        item.CausationId |> valueOrDbNull, uuidParam
                        item.CorrelationId  |> valueOrDbNull, uuidParam
                        item.Data |> box, jsonBParam
                        item.Metadata |> valueOrDbNull, jsonBParam
                        // item.CreatedAt |> box, timestampParam

                    ]
                let offset = index * Seq.length pms
                let parmeterizedString =
                    pms
                    |> Seq.mapi ^ fun index (value, paramCtor) ->
                        let paramName = offset + index |> string
                        value |> paramCtor paramName |> addParameter cmd //SIDE EFFECT!!!
                        paramName |> String.format "@{0}"
                    |> String.join ","
                    |> String.format "({0})"
                parmeterizedString
            |> String.join ","

        cmd.CommandText <-
            buildParameters ()
            |> String.format
                    """
                    INSERT INTO events
                    (
                      event_type,
                      causation_id,
                      correlation_id,
                      data,
                      metadata
                    )
                    VALUES {0}
                    RETURNING event_id;
                    """
        // printfn "%A" cmd.CommandText
        cmd

    let insertEvents conn (events : DbTypes.RecordedEvent seq) = job {
         use cmd = prepareInsertEvents conn events
         use! reader = cmd |> executeReader
         return!
             reader
             |> mapRow ^ fun row ->
                match row with
                | ["event_id", Some id] ->
                    id |> unbox<Guid> |> Some
                | _ -> None
     }



module Commands =
    open System
    open Hopac
    open Npgsql
    open DbHelpers
    open DomainTypes
    let AllStreamId = 0

    type AppendResult = {
        EventIds : Guid seq
        NextExpectedVersion : uint64
    }
    exception ConcurrencyException of string

    let internal concurrencyCheck conn streamName version = job {
        let! streamOpt = //Job.benchmark "getStreamByName" ^ fun () ->
                    Repository.getStreamByName conn streamName
        let createStream () =
            Repository.prepareCreateStream conn streamName
            |> DbHelpers.executeNonQuery
            |> Job.bind ^ fun _ ->
                Repository.getStreamByName conn streamName
            |> Job.map Option.get
        return!
            // https://eventstore.org/docs/dotnet-api/4.0.2/optimistic-concurrency-and-idempotence/
            //TODO: Result type?
            match streamOpt, version with
            | Some stream, Any ->
                stream |> Job.result
            | None, Any ->
                createStream ()
            | Some _, NoStream ->
                streamName |> sprintf "Expected stream %s to not have been created yet. " |> ConcurrencyException |> raise
            | None , NoStream ->
                createStream ()
            | Some stream, StreamExists ->
                stream |> Job.result
            | None , StreamExists ->
                streamName |> sprintf "Expected stream %s to have already been created. " |> ConcurrencyException |> raise
            | Some stream, EmptyStream when stream.Version <> 0UL ->
                (streamName,stream.Version) ||> sprintf "Stream %s expected to be at version 0 but at %d" |> ConcurrencyException |> raise
            | Some stream, EmptyStream ->
                stream |> Job.result
            | None, EmptyStream ->
                streamName |> sprintf "Stream %s expected to have been created but was not" |> ConcurrencyException |> raise
            | Some stream, Version.Value version when stream.Version = version ->
                stream |> Job.result
            | Some stream, Version.Value version ->
                sprintf "Stream %s expected to be at version %d but at %d" streamName version stream.Version |> ConcurrencyException |> raise
            | None, Version.Value 0UL ->
                createStream ()
            | None, Version.Value _ ->
                sprintf "Expected stream %s to have been created." streamName  |> ConcurrencyException |> raise

    }


    let appendToStream' (connStr : NpgsqlConnectionStringBuilder) streamName (version : Version) events = job {
        use conn = builderToConnection connStr
        do! conn |> ensureOpen
        use transaction = conn.BeginTransaction()

        let! stream = concurrencyCheck conn streamName version


        let! savedEvents = Repository.insertEvents conn events
        use cmd = Repository.prepareCreateStreamEvents conn stream.Id (savedEvents)
        let! nextExpectedVersion = cmd |> executeScalar<int64>

        use cmd = Repository.prepareLinkEvents conn AllStreamId (savedEvents)
        do! cmd |> executeNonQueryIgnore

        do! transaction.CommitAsync()
            |> Job.awaitUnitTask

        return {
            NextExpectedVersion = nextExpectedVersion |> uint64
            EventIds = savedEvents
        }
    }
    let linkToStream' (connStr : NpgsqlConnectionStringBuilder) streamName (version : Version) eventIds = job {
        use conn = builderToConnection connStr
        do! conn |> ensureOpen
        use transaction = conn.BeginTransaction()

        let! stream = concurrencyCheck conn streamName version

        use cmd = Repository.prepareLinkEvents conn stream.Id (eventIds)
        let! nextExpectedVersion = cmd |> executeScalar<int64>

        do! transaction.CommitAsync()
            |> Job.awaitUnitTask

        return {
            NextExpectedVersion = nextExpectedVersion |> uint64
            EventIds = eventIds
        }
    }

    open Hopac.Infixes
    type Msg =
    | AppendToStream of DomainTypes.StreamName*Version*DbTypes.RecordedEvent array*Actors.ReplyChannel<Choice<AppendResult,exn>>
    | LinkToStream of StreamName*Version*seq<EventId>*Actors.ReplyChannel<Choice<AppendResult,exn>>

    //TODO make singleton
    let create connectionString =
        Actors.actor <|
            fun mb -> Job.foreverServer (Mailbox.take mb >>= function
                | AppendToStream(name,version,events, reply) ->
                    job {
                        let! result = appendToStream' connectionString name version events |> Job.catch
                        do! Actors.reply reply result
                    }
                | LinkToStream(name,version,eventIds, reply) ->
                    job {
                        let! result = linkToStream' connectionString name version eventIds |> Job.catch
                        do! Actors.reply reply result
                    }
            )

    let appendToStream name version events actor=
        Actors.postAndReply actor (fun i -> AppendToStream(name,version,events,i))
        |> Job.map ^ function
            | Choice1Of2 r -> r
            | Choice2Of2 ex ->
                Ex.throwPreserve ex

    let linkToStream (name : StreamName) (version : Version) ( eventIds : EventId seq) actor =
        Actors.postAndReply actor (fun i -> LinkToStream(name,version,eventIds,i))
        |> Job.map ^ function
            | Choice1Of2 r -> r
            | Choice2Of2 ex ->
                Ex.throwPreserve ex



module Subscriptions =
    open DbHelpers
    open System
    open System.Linq
    open Hopac
    open Npgsql
    open System.Threading
    // initial -> subscribe_to_events -> request_catch_up -> catching_up -> subscribed

    type SubscriptionState =
    | Initial
    | SubscribeToEvents
    | RequestCatchUp
    | Catchup
    | Subscribed

    type Notification = {
        StreamName : string
        StreamId : uint64
        FirstWriteVersion: uint64
        LastWriteVersion: uint64
    }
    with
        static member parse (notification : string) =

            let parts =
                notification
                |> String.reverse
                |> String.splitByCharMax ',' 4
                |> Array.map (String.reverse)
            {
                StreamName = parts.[3]
                StreamId   = parts.[2] |> uint64
                FirstWriteVersion = parts.[1] |> uint64
                LastWriteVersion = parts.[0] |> uint64
            }

    let startNotify (ct : CancellationToken) connStr = job {
            // -- Payload text contains:
            //             --  * `stream_name`
            //             --  * `stream_id`
            //             --  * first `stream_version`
            //             --  * last `stream_version`
            //             -- Each separated by a comma (e.g. 'stream-12345,1,1,5')
        let connStr = NpgsqlConnectionStringBuilder(connStr |> string , KeepAlive = 30)
        let output = Stream.Src.create<NpgsqlNotificationEventArgs>()
        let conn = builderToConnection connStr
        do! ensureOpen conn
        use cmd = new NpgsqlCommand(sprintf "LISTEN \"%s\"" "events", conn)
        do! cmd |> executeNonQueryIgnore

        let notificationSub =
            conn.Notification.Subscribe (Stream.Src.value output >> start)

        job {
            let cleanup = job {
                // Wait for connection to real stop fetching before closing
                //https://github.com/npgsql/npgsql/issues/1638#issuecomment-357933587
                let rec spinWaitConnectionFinished count i = job {
                    if count = i
                       || (conn.FullState.HasFlag(System.Data.ConnectionState.Fetching) |> not) then
                        ()
                    else
                        do! timeOutMillis (i * 10)
                        return! spinWaitConnectionFinished count (i + 1)
                }
                notificationSub.Dispose()
                do! spinWaitConnectionFinished 50 0
                conn.Dispose()
                do! Stream.Src.close output
            }
            // need to loop forever to get notifications
            // https://github.com/npgsql/npgsql/issues/1024
            while ct.IsCancellationRequested |> not do
                    try
                        do! conn.WaitAsync(ct) |> Job.awaitUnitTask
                    with
                    | null ->
                        // TODO: Chase down null exeception
                        // Somehow hopac or npgsql throws a null exception, not a nullreference exception
                        // It might be an interaction of how npgsql cancels it's operation
                        // and hopac handles cancelled tasks
                        ()
                    | e ->
                        do! cleanup
                        Ex.throwCapture e
            do! cleanup
            return! Job.abort()
        }
        |> server

        return output
               |> Stream.Src.tap
               |> Stream.mapFun(fun arg -> Notification.parse arg.AdditionalInformation)
    }
// module Fooy =
//     open System
//     open System.Collections.Generic
//     open Npgsql
//     open Hopac
//     open Hopac.Infixes
//     open System.Data
//     open DbTypes

//     let (^) x = (<|) x



//     type Callback = (RecordedEvent -> Job<unit>)

//     type Channel = string
//     type Messages =
//     | Subscribe of Guid * Channel * Callback
//     | Unsubscribe of Guid
//     | NewEvents of Channel * RecordedEvent array

//     let ensureOpened (conn : NpgsqlConnection) = job {
//         if conn.State <> ConnectionState.Open then
//             do! conn.OpenAsync() |> Job.awaitUnitTask
//     }

//     let listenOn conn channel = job {
//         use cmd = new NpgsqlCommand(sprintf "LISTEN \"%s\"" channel, conn)
//         do! cmd.ExecuteNonQueryAsync() |> Job.awaitTask |> Job.Ignore
//     }
//     let unlistenOn conn channel = job {
//         use cmd = new NpgsqlCommand(sprintf "UNLISTEN \"%s\"" channel, conn)
//         do! cmd.ExecuteNonQueryAsync() |> Job.awaitTask |> Job.Ignore
//     }

//     type Notifier<'a> =
//       private {putCh : Ch<'a>}

//     let doLookup (args : NpgsqlNotificationEventArgs) = job {
//         return NewEvents(args.Condition,[|RecordedEvent.Empty|])
//         // return args.AdditionalInformation
//     }

//     let sendMessage (n : Notifier<_>) msg =
//         n.putCh *<- msg

//     let create (connBuilder : NpgsqlConnectionStringBuilder) = job {
//             let connBuilder = NpgsqlConnectionStringBuilder(connBuilder |> string , KeepAlive = 30)
//             let conn = new NpgsqlConnection(connBuilder |> string)
//             do! ensureOpened conn
//             let self = { putCh = Ch ();}

//             let subscriptions = Dictionary<Channel, (Guid * Callback) ResizeArray>()

//             conn.Notification.Add(doLookup >=> (sendMessage self >> asJob) >> start)

//             let handleMessage msg = job {
//                 match msg with
//                 | Subscribe (uuid, channel, callback) ->
//                     match subscriptions |> Dictionary.tryGet channel with
//                     | Some subs ->
//                         subs.RemoveAll(fun (subId,_) -> uuid = subId) |> ignore
//                         subs.Add(uuid,callback)
//                     | None ->
//                         let subs = ResizeArray<_>()
//                         subs.Add(uuid,callback)
//                         subscriptions.Add(channel,subs)
//                         do! listenOn conn channel

//                 | Unsubscribe uuid ->
//                     match subscriptions |> Dictionary.tryFind(fun key value -> value |> Seq.exists(fun (subId,_) -> subId = uuid)) with
//                     | Some (channel, subs) ->
//                         subs.RemoveAll(fun (subId,_) -> uuid = subId) |> ignore
//                         if subs |> Seq.exists (fun (subId,_) -> uuid = subId) |> not then
//                             do! listenOn conn channel
//                             subscriptions.Remove(channel) |> ignore

//                     | None ->
//                         // Nothing to remove
//                         ()

//                 | NewEvents (channel,msgs) ->
//                     match subscriptions |> Dictionary.tryGet channel with
//                     | Some (subs) ->
//                         do!
//                             subs
//                             |> Seq.collect ^ fun (_,callback) -> msgs |> Array.map callback
//                             |> Job.conIgnore
//                     | None ->
//                         ()
//             }
//             let put () =
//                 self.putCh ^=> handleMessage
//             let proc = Job.delay ^ fun () ->
//                 put ()

//             return! Job.foreverServer proc >>-. self

//             // return self
//     }



// module Say =

//     let hello name =
//         sprintf "Hello %s" name

module Eventstore =
    open System
    open Npgsql
    open System.Threading
    open Hopac
    open DomainTypes

    type Projection<'a> =
    // Typically used to build a read model from a stream.  This readmodel in theory should be able to be rebuilt.
    | ReadModel of (DbTypes.RecordedEvent -> Job<'a>)
    // Typically used for side effects for a stream, like linking events to another stream or send emails.  This typically is not something you can easy undo.
    | PartitionTo of (DbTypes.RecordedEvent -> Job<unit>)

    type Ack = IVar<unit>

    type Eventstore(connString : NpgsqlConnectionStringBuilder, writer) =

        let cts = new CancellationTokenSource()
        let appendToStream streamName version events  =
            Commands.appendToStream streamName version events  writer
        let linkToStream streamName version eventIds =
            Commands.linkToStream streamName version eventIds writer
        let readLimit = 1000UL //TODO: Configuration?
        let readStreamFoward name startingPosition =
            Repository.readEventsForward cts.Token connString name startingPosition readLimit


        let mutable diposeLock = obj()
        member __.Dispose () =
            let disposer = Interlocked.Exchange<obj>(&diposeLock, null)
            if disposer |> isNull |> not then cts.Cancel()

        member __.IsDisposed = diposeLock |> isNull

        member __.AppendToStream =
            appendToStream

        member __.LinkToStream =
            linkToStream

        member __.GetStreamInfo steamName = job {
            use conn =  connString |> DbHelpers.builderToConnection
            do! conn |> DbHelpers.ensureOpen
            let! result = Repository.getStreamByName conn steamName
            return result
        }

        member __.ReadFowardFromStream =
            readStreamFoward

        member __.SubscribeToStream streamName subscriptionName (subscriptionPosition : SubscriptionPosition)= job {
            let src = Stream.Src.create<Ack*DbTypes.RecordedEvent>()
            return Stream.Src.tap src
        }
        member __.UnsubscribeToStream streamName subscriptionName (subscriptionPosition : SubscriptionPosition)= job {
            ()
        }

        static member Create (connString : NpgsqlConnectionStringBuilder) = job {
            let! writer = Commands.create connString
            return new Eventstore(connString,writer)
        }

        interface IDisposable with
            member __.Dispose() = __.Dispose()
