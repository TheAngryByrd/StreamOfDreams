namespace StreamOfDreams

open System
open System.Threading
open System.Reactive.Disposables
open System.Net.Mail
open Migrations

[<AutoOpen>]
module Infrastructure =
    let (^) x = (<|) x


module Job =
    open Hopac
    let inline usingJob (xJ : Job<'a>) (xJyJ : 'a -> Job<'b>)=
        xJ
        |> Job.bind ^ fun dis ->
            Job.using dis xJyJ

    let inline usingJob' (xJyJ : 'a -> Job<'b>) (xJ : Job<'a>) =
        usingJob xJ xJyJ


    let benchmark name (f : unit -> Job<'a>) = job {
        let sw = System.Diagnostics.Stopwatch.StartNew()
        let! result = f ()
        sw.Stop()
        printfn "%s took %A ms" name sw.ElapsedMilliseconds
        return result
    }


module Dictionary =
    open System.Collections.Generic

    let inline tryGet key (dict : IDictionary<_,_>) =
        match dict.TryGetValue(key) with
        | (true,v) -> Some v
        | _ -> None

    let inline tryFind predicate (dict : IDictionary<_,_>) =
        dict
        |> Seq.tryFind(fun kvp -> predicate kvp.Key kvp.Value)
        |> Option.map(fun kvp -> kvp.Key, kvp.Value)


module String =
    open System

    let inline format template (replacement : obj)=
        String.Format(template, replacement)

    let inline formatMany template (replacement : obj array)=
        String.Format(template, replacement)

    let inline join (seperator : string) (strings : string seq) = String.Join(seperator, strings)

    let inline toCamelCase (phrase : string) =
        let doWork (char : char) (phrase : string) =
            phrase.Split char
            |> Seq.map (fun p -> Char.ToLower(p.[0]).ToString() + p.Substring(1))
            |> join (char.ToString())
        phrase
        |> doWork '.'
        |> doWork '+'

module TypeInfo =
    open System

    let inline getType obj = obj.GetType()
    let inline getTypeName (``type`` : Type) = ``type``.Name
    let inline getTypeFullName (``type`` : Type) = ``type``.FullName

    let inline typeNameToCamelCase obj =
        obj
        |> getType
        |> getTypeName
        |> String.toCamelCase

module Json =
    open Newtonsoft.Json

    let inline serialize o =
        JsonConvert.SerializeObject o

    let inline deserialize<'a> o =
        JsonConvert.DeserializeObject<'a> o


module DomainTypes =
    type StreamName = string

module DbHelpers =
    open System
    open Npgsql
    open NpgsqlTypes
    open Hopac

    let builderToConnection (connStr : NpgsqlConnectionStringBuilder) =
        new NpgsqlConnection (connStr |> string)

    let inline ensureOpen (connection : NpgsqlConnection) = job {
        if not <| connection.FullState.HasFlag System.Data.ConnectionState.Open then
            do! connection.OpenAsync() |> Job.awaitUnitTask
    }

    let inline executeReader (cmd : NpgsqlCommand) =
        Job.fromTask cmd.ExecuteReaderAsync
        |> Job.map (unbox<NpgsqlDataReader>)

    let inline executeScalar<'a> (cmd : NpgsqlCommand) =
        Job.fromTask cmd.ExecuteScalarAsync
        |> Job.map (unbox<'a>)

    let inline executeNonQuery (cmd : NpgsqlCommand) =
        Job.fromTask cmd.ExecuteNonQueryAsync

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

    let inline readTable (reader: NpgsqlDataReader) =
        let rec readRows rows = job {
            let! canRead = reader.ReadAsync()
            if canRead then
              let! row = readRow reader
              return! readRows (List.ofSeq row :: rows)
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

    type Stream = {
        Id : int64
        Name : StreamName
        Version : uint64
        CreatedAt : DateTime
    }

    type Event = {
        Id : Guid
        EventType : string
        CorrelationId: Guid option
        CausationId: Guid option
        Data : string
        Metadata : string option
    }
        with
            static member CreateSimple eventType data =
                {
                    Id = Guid.NewGuid()
                    EventType = eventType
                    CorrelationId = None
                    CausationId = None
                    Data = Json.serialize data
                    Metadata = None
                }
            static member AutoTypeData data =
                {
                    Id = Guid.NewGuid()
                    EventType = TypeInfo.typeNameToCamelCase data
                    CorrelationId = None
                    CausationId = None
                    Data = Json.serialize data
                    Metadata = None
                }
            static member AutoTypeDataAndMeta data meta =
                {
                    Id = Guid.NewGuid()
                    EventType = TypeInfo.typeNameToCamelCase data
                    CorrelationId = None
                    CausationId = None
                    Data = Json.serialize data
                    Metadata = Json.serialize meta |> Some
                }

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
            RETURNING stream_id;
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
                  RETURNING stream_version - @eventCount as initial_stream_version
                ),
                events (index, event_id) AS (
                  VALUES {0}
                )
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
              FROM events, stream;
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
                          RETURNING stream_version - @eventCount as initial_stream_version
                        ),
                        events (index, event_id) AS (
                          VALUES {0}
                        )
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
                          AND original_stream_events.stream_id = original_stream_events.original_stream_id;
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
    let prepareReadFoward conn streamId version limit=
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
            ORDER BY se.stream_version DESC
            LIMIT @limit
            """

        streamId |> inferredParam "streamId" |> addParameter cmd
        version |> inferredParam "version" |> addParameter cmd
        limit |> inferredParam "limit" |> addParameter cmd
        cmd

    let readEventsForward conn (streamName : StreamName) startVersion limit = job {
        // let limit = 3000
        let getBatch streamId version = job {
            use cmd = prepareReadFoward conn streamId version limit
            use! reader = cmd |> executeReader
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

        let! streamOpt = getStreamByName conn streamName
        match streamOpt with
        | Some s ->
            let xs =
                Stream.unfoldJob ^ fun (start : int) ->
                    job {
                        let! retval = getBatch s.Id start
                        let returnedLength = retval |> Seq.length
                        if returnedLength = 0 then
                            return None
                        else
                            return Some (retval, (start + limit))
                }
            return Some (xs startVersion |> Stream.appendMap (Stream.ofSeq))
        | None ->
            return None

        // let! streamOpt = getStreamByName conn streamName
        // return!
        //     match streamOpt with
        //     | Some s -> job {
        //         use cmd = prepareReadFoward conn s.Id startVersion
        //         let! reader = cmd |> executeReader
        //         let! results =
        //             reader
        //             |> mapRow ^ function
        //                 | [ "stream_version", Some streamVersion
        //                     "event_id", Some eventId
        //                     "stream_name", Some streamName
        //                     "original_stream_version", Some originalVersion
        //                     "event_type", Some eventType
        //                     "correlation_id", correlation
        //                     "causation_id", causatoin
        //                     "data", Some data
        //                     "metadata", metadata
        //                     "created_at", Some createdAt ]
        //                         -> Some <|
        //                             {
        //                                 Id =  eventId |> unbox<Guid>
        //                                 Number = originalVersion |> unbox<int64> |> uint64
        //                                 StreamName =  streamName |> unbox<string>
        //                                 StreamVersion = originalVersion |> unbox<int64> |> uint64
        //                                 CorrelationId= correlation |> unbox<Guid option>
        //                                 CausationId = causatoin |> unbox<Guid option>
        //                                 EventType=  eventType |> unbox<string>
        //                                 Data = data |> unbox<string>
        //                                 Metadata=  metadata |> unbox<string option>
        //                                 CreatedAt = createdAt |> unbox<DateTime>

        //                             }
        //                 | _ ->
        //                    // printfn "%A" f
        //                     None
        //         return Some results
        //         }
        //     | None ->
        //         //TODO: Result Type
        //         None |> Job.result
    }



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
    open Hopac
    open Npgsql
    open DbHelpers

    let AllStreamId = 0
    type Version =
    | Start
    | Any
    | Value of uint64

    //TODO: Single writer, Actor
    let appendToStream (connStr : NpgsqlConnectionStringBuilder) streamName version events = job {
        use conn = builderToConnection connStr
        do! conn |> ensureOpen
        use transaction = conn.BeginTransaction()

        let! streamOpt = //Job.benchmark "getStreamByName" ^ fun () ->
                            Repository.getStreamByName conn streamName

        let! stream =
            match streamOpt with
            | Some s ->
                s |> Job.result
            | None ->
                //Job.benchmark "prepareCreateStream" ^ fun () ->
                    Repository.prepareCreateStream conn streamName
                    |> DbHelpers.executeNonQuery
                    |> Job.bind ^ fun _ ->
                        Repository.getStreamByName conn streamName
                    |> Job.map Option.get

        //TODO: Figure out version checking
        let nextVersion =
            match version with
            | Any -> stream.Version + 1UL
            | Start ->
                if stream.Version <> 0UL then
                    failwithf "Concurrency error. Stream expected to be started but at %A" stream.Version
                0UL
            | Value expectedVersion ->
                if  expectedVersion > stream.Version then
                    failwithf "Concurrency error.  Expected version higher than %A but got %A" stream.Version expectedVersion
                expectedVersion

        let! savedEvents =
           // Job.benchmark "insertEvents" ^ fun () ->
                Repository.insertEvents conn events


        let! result =
            let foo = Repository.prepareCreateStreamEvents conn stream.Id (savedEvents)
            //Job.benchmark "prepareCreateStreamEvents" ^ fun _ ->
            foo |> executeNonQuery
        let! result =
            let foo = Repository.prepareLinkEvents conn AllStreamId (savedEvents)
            //Job.benchmark "prepareLinkEvents" ^ fun _ ->
            foo |> executeNonQuery
        do! transaction.CommitAsync() |> Job.awaitUnitTask
        //lookup stream
        ()
        // ensure version
        // create if not exist

        //map events to recorded events
        //
        // link to $all

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
