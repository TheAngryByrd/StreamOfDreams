namespace StreamOfDreams

open StreamOfDreams.Logging

[<AutoOpen>]
module Log =
    open Hopac
    open System.Diagnostics
    let internal logger = Log.create "StreamOfDreams"


    let timeJob pointName (fn : 'input -> Job<'res>) : 'input -> Job<'res> =
        fun input ->
          job {
            let sw = Stopwatch.StartNew()
            let! res = fn input
            sw.Stop()

            // https://github.com/logary/logary/blob/master/src/adapters/Logary.Adapters.Facade/Logary.Adapters.Facade.fs#L91
            let message =
                Message.gauge (sw.ElapsedMilliseconds) "ms"
                |> Message.setName [|pointName|]
            logger.logSimple message
            return res
          }

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
    /// Use what was persisted last in the database.  This useful for processors that want to start from where they left off. Will start at 0 if no subscription was previously set.
    | Continue
    /// Use a discrete value.  Useful for if you know exactly where you want to start.  Should not be used for long running processes.
    | Value of uint64




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
    type Subscription = {
        Id : int64
        StreamName : string
        Name : string
        LastSeen : uint64
        CreatedAt : DateTime
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

    let getStreamByName ct conn (streamName : StreamName) = job {
        logger.verbose(
            Message.eventX "Getting info for Streamname:{streamName}"
            >> Message.setField "streamName" streamName
        )
        use cmd = preparestreamIdAndVersion conn streamName
        use! reader = cmd |> executeReaderCt ct

        let! rowOpt = readFirstRow ct (reader)

        let retVal =
            rowOpt
            |> Option.bind ^
                function
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


    let prepareCreateSubscription conn streamName subscriptionName lastSeen=
        let cmd = new NpgsqlCommand(Connection = conn)
        cmd.CommandText <-
            """
            INSERT INTO subscriptions (stream_name, subscription_name, last_seen)
            VALUES (@streamName, @subscriptionName, @lastSeen)
            ON CONFLICT DO NOTHING;

            SELECT subscription_id, stream_name, subscription_name, last_seen,created_at
            FROM subscriptions
            WHERE stream_name = @streamName AND subscription_name = @subscriptionName;
            """
        streamName |> textParam "streamName" |> addParameter cmd
        subscriptionName |> textParam "subscriptionName" |> addParameter cmd
        lastSeen |> bigintParam "lastSeen" |> addParameter cmd
        cmd

    let insertAndReadSubscription ct connStr streamId subscriptionName lastSeen = job {
        use! conn = connStr |> createOpenConnectionCt ct
        use cmd = prepareCreateSubscription conn streamId subscriptionName lastSeen
        use! reader = cmd |> executeReaderCt ct
        let! rowOpt = reader |> DbHelpers.readFirstRow ct
        return
            rowOpt
            |> Option.bind ^
                 function
                    | [ "subscription_id", Some id
                        "stream_name", Some streamName
                        "subscription_name", Some subscriptionName
                        "last_seen", Some lastSeen
                        "created_at", Some date
                        ] ->
                            Some <| {
                            Id = id |> unbox<int64>
                            StreamName = streamName |> unbox<string>
                            Name = subscriptionName |> unbox<string>
                            LastSeen = lastSeen |> unbox<int64> |> uint64
                            CreatedAt = date |> unbox<DateTime>
                        }
                    | _ -> None


    }


    let prepateUpdateLastSeenSubscription conn subscriptionId lastSeen =
        let cmd = new NpgsqlCommand(Connection = conn)
        cmd.CommandText <-
            """
            UPDATE subscriptions
            SET last_seen = @lastSeen
            WHERE subscription_id = @subscriptionId;
            """
        subscriptionId |> inferredParam "subscriptionId" |> addParameter cmd
        lastSeen |> bigintParam "lastSeen" |> addParameter cmd
        cmd


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
            ORDER BY se.stream_version ASC
            LIMIT @limit
            """

        streamId |> inferredParam "streamId" |> addParameter cmd
        version |> int64 |> inferredParam "version" |> addParameter cmd
        limit |> int64 |> inferredParam "limit" |> addParameter cmd
        cmd

    let readEventsForwardInner (forever : bool) token (connStr : NpgsqlConnectionStringBuilder) (streamName : StreamName) (startVersion : uint64) (limit : uint64) = job {
        // let limit = 3000

        let getBatch streamId version = job {
            use! conn = connStr |> createOpenConnectionCt token
            use cmd = prepareReadFoward conn streamId version limit
            use! reader = cmd |> executeReaderCt token
            let! results =
                reader
                |> mapRow token ^ function
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
                                    Number = originalVersion |> unbox<int64> |> uint64
                                    Id =  eventId |> unbox<Guid>
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
            // printfn "readEventsForwardInner: %A" results
            return results
        }

        let! streamOpt = job {
            use! conn = connStr |> createOpenConnectionCt token
            return! getStreamByName token conn streamName
        }

        match streamOpt with
        | Some s ->
            let xs =
                Stream.unfoldJob ^ fun (start : uint64) ->
                    job {
                        // printfn "getting batch start %A" start
                        if token.IsCancellationRequested then
                            return None
                        else
                            let! retval = getBatch s.Id start
                            let returnedLength = retval |> Seq.length
                            if returnedLength = 0 then
                                if forever then
                                    do! timeOutMillis 250
                                    return Some ([], (start))
                                else
                                    return None
                            else
                                return Some (retval, (start + uint64 returnedLength))
                    }

                // >> Stream.doFinalizeFun (fun _ -> token.)
            return Some (xs startVersion)
        | None ->
            return None
    }

    let readEventsForwardForever token (connStr : NpgsqlConnectionStringBuilder) (streamName : StreamName) (startVersion : uint64) (limit : uint64) =
        readEventsForwardInner true token connStr streamName startVersion limit
        |> Job.map (Option.map(Stream.appendMap (Stream.ofSeq)))

    let readEventsForward token (connStr : NpgsqlConnectionStringBuilder) (streamName : StreamName) (startVersion : uint64) (limit : uint64) =
       readEventsForwardInner false token connStr streamName startVersion limit
       |>Job.map (Option.map(Stream.appendMap (Stream.ofSeq)))

    let readEventsForwardBatched token (connStr : NpgsqlConnectionStringBuilder) (streamName : StreamName) (startVersion : uint64) (limit : uint64) =
       readEventsForwardInner false token connStr streamName startVersion limit


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

    let insertEvents ct conn (events : DbTypes.RecordedEvent seq) = job {
         use cmd = prepareInsertEvents conn events
         use! reader = cmd |> executeReaderCt ct
         return!
             reader
             |> mapRow ct ^ fun row ->
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

    let internal concurrencyCheck ct conn streamName version = job {
        let! streamOpt = //Job.benchmark "getStreamByName" ^ fun () ->
                    Repository.getStreamByName ct conn streamName
        let createStream () =
            Repository.prepareCreateStream conn streamName
            |> DbHelpers.executeNonQueryCt ct
            |> Job.bind ^ fun _ ->
                Repository.getStreamByName ct conn streamName
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


    let appendToStream' ct (connStr : NpgsqlConnectionStringBuilder) streamName (version : Version) events = job {
        use! conn = createOpenConnectionCt ct connStr
        use transaction = conn.BeginTransaction()

        let! stream = concurrencyCheck ct conn streamName version


        let! savedEvents = Repository.insertEvents ct conn events
        use cmd = Repository.prepareCreateStreamEvents conn stream.Id (savedEvents)
        let! nextExpectedVersion = cmd |> executeScalarCt<int64> ct

        use cmd = Repository.prepareLinkEvents conn AllStreamId (savedEvents)
        do! cmd |> executeNonQueryIgnoreCt ct

        do! transaction.CommitAsync(ct)
            |> Job.awaitUnitTask

        return {
            NextExpectedVersion = nextExpectedVersion |> uint64
            EventIds = savedEvents
        }
    }
    let linkToStream' ct (connStr : NpgsqlConnectionStringBuilder) streamName (version : Version) eventIds = job {
        use! conn = createOpenConnectionCt ct connStr
        use transaction = conn.BeginTransaction()

        let! stream = concurrencyCheck ct conn streamName version

        use cmd = Repository.prepareLinkEvents conn stream.Id (eventIds)
        let! nextExpectedVersion = cmd |> executeScalarCt<int64> ct

        do! transaction.CommitAsync(ct)
            |> Job.awaitUnitTask

        return {
            NextExpectedVersion = nextExpectedVersion |> uint64
            EventIds = eventIds
        }
    }

    let linkToStream'' ct conn streamName (version : Version) eventIds = job {
        let! stream = concurrencyCheck ct conn streamName version

        use cmd = Repository.prepareLinkEvents conn stream.Id (eventIds)
        let! nextExpectedVersion = cmd |> executeScalarCt<int64> ct

        return {
            NextExpectedVersion = nextExpectedVersion |> uint64
            EventIds = eventIds
        }
    }

    type Batch = seq<StreamName*Version*seq<EventId>>
    open Hopac.Infixes
    type Msg =
    | AppendToStream of DomainTypes.StreamName*Version*DbTypes.RecordedEvent array*Actors.ReplyChannel<Choice<AppendResult,exn>>
    | LinkToStream of StreamName*Version*seq<EventId>*Actors.ReplyChannel<Choice<AppendResult,exn>>
    | LinkToStreamTransaction of NpgsqlConnection*Promise<unit>*StreamName*Version*seq<EventId>*Actors.ReplyChannel<Choice<AppendResult,exn>>
    | LinkToStreamTransactionBatch of NpgsqlConnection*Promise<unit>*Batch*Actors.ReplyChannel<Choice<ResizeArray<AppendResult>,exn>>
    //TODO make singleton

    let private logAmount pointName amount =
        Message.gauge (amount) ""
                        |> Message.setName pointName
                        |> logger.logSimple
    let create ct connectionString =
        Actors.actor <|
            fun mb -> Job.foreverServer (Mailbox.take mb >>= function
                | AppendToStream(name,version,events, reply) ->
                    job {
                        logAmount [|"appendToStream'"|] (events |> Seq.length |> int64)
                        let! result =
                            timeJob "appendToStream'" (fun () ->
                               appendToStream' ct connectionString name version events |> Job.catch
                            ) ()
                        do! Actors.reply reply result
                    }
                | LinkToStream(name,version,eventIds, reply) ->
                    job {
                        logAmount [|"linkToStream'"|] (eventIds |> Seq.length |> int64)
                        let! result =
                            timeJob "linkToStream'" (fun () ->
                                linkToStream' ct connectionString name version eventIds |> Job.catch
                            ) ()

                        do! Actors.reply reply result
                    }
                | LinkToStreamTransaction(conn,doneAck, name,version,eventIds, reply) ->
                    job {
                        logAmount [|"linkToStream''"|] (eventIds |> Seq.length |> int64)
                        let! result =
                            timeJob  "linkToStream''" (fun () ->
                                linkToStream'' ct conn name version eventIds |> Job.catch
                            ) ()
                        do! Actors.reply reply result
                        do! doneAck
                    }
                | LinkToStreamTransactionBatch(conn,doneAck, batch, reply) ->
                    job {
                        let! results =
                            timeJob  "linkToStream''.batch" (fun () ->
                                batch
                                |> Seq.map ^ fun (name,version,eventIds) -> job {
                                    logAmount [|"linkToStream''.batch"|] (eventIds |> Seq.length |> int64)
                                    return! linkToStream'' ct conn name version eventIds
                                }
                                |> Job.seqCollect
                                |> Job.catch ) ()
                        do! Actors.reply reply results
                        do! doneAck
                    }
            )

    let appendToStream name version events actor=
        Actors.postAndReply actor (fun i -> AppendToStream(name,version,events,i))
        |> Job.map ^ function
            | Choice1Of2 r -> r
            | Choice2Of2 ex ->
                Log.logger.error(
                    Message.eventX "appendToStream failure! {name} - {version}"
                    >> Message.setField "name" name
                    >> Message.setField "version" version
                    >> Message.addExn ex
                )
                Ex.throwPreserve ex

    let linkToStream (name : StreamName) (version : Version) ( eventIds : EventId seq) actor =
        Actors.postAndReply actor (fun i -> LinkToStream(name,version,eventIds,i))
        |> Job.map ^ function
            | Choice1Of2 r -> r
            | Choice2Of2 ex ->
                Ex.throwPreserve ex

    let linkToStreamTransaction conn doneAck (name : StreamName) (version : Version) ( eventIds : EventId seq) actor =
        Actors.postAndReply actor (fun i -> LinkToStreamTransaction(conn, doneAck, name,version,eventIds,i))
        |> Job.map ^ function
            | Choice1Of2 r -> r
            | Choice2Of2 ex ->
                Ex.throwPreserve ex

    let linkToStreamTransactionBatch conn doneAck batch actor =
        Actors.postAndReply actor (fun i -> LinkToStreamTransactionBatch(conn, doneAck, batch,i))
        |> Job.map ^ function
            | Choice1Of2 r -> r
            | Choice2Of2 ex ->
                Ex.throwPreserve ex

module Subscriptions =

    open DbHelpers
    open System
    open System.Linq
    open Hopac
    open Hopac.Infixes
    open Npgsql
    open System.Threading
    open DomainTypes

    /// https://i.imgur.com/xw9yypr.jpg
    /// Alias for `Job<unit>` that a subscriber recieved an handled an event.
    type Ack = Promise<unit>

    /// A notfication event.  This gets fired after each update to the `streams` table.
    type Notification = {
        /// Name of the stream with new events either added or linked
        StreamName : string
        /// Id of the stream with new events either added or linked
        StreamId : uint64
        /// Previous version of the stream
        LastVersion : uint64
        /// The position of the last event written in this batch
        LastWriteVersion: uint64
    }
    with
        /// The position of the first event written in this batch.  Should always be `LastVersion` + 1.
        member __.FirstWriteVersion =
            __.LastVersion + 1UL
        /// The number of events written in this batch
        member __.EventsWritten =
            __.LastWriteVersion - __.LastVersion
        /// Parses notification events from postgres.  Takes the form of `stream-12345,1,1,5`
        static member parse (notification : string) =

            let parts =
                notification
                |> String.reverse
                |> String.splitByCharMax ',' 4
                |> Array.map (String.reverse)
            {
                StreamName = parts.[3]
                StreamId   = parts.[2] |> uint64
                LastVersion =  parts.[1] |> uint64
                LastWriteVersion = parts.[0] |> uint64
            }

    type Subscriber = { notification : Ch<Notification> ; }

    module Subsciption =
        open DomainTypes
        open DbTypes
        let create
            ct
            (connStr : NpgsqlConnectionStringBuilder)
            (streamName)
            (subscriptionName)
            (subscriptionPosition : SubscriptionPosition)
            (output : Stream.Src<NpgsqlConnection*Ack*RecordedEvent seq*Promise<unit>>)
            readLimit = job {

                //TODO load or create subscription data
                let position =
                    match subscriptionPosition with
                    | Continue -> 0UL
                    | Beginning -> 0UL
                    | Value v -> v
                let! subscription =
                    Repository.insertAndReadSubscription ct connStr streamName subscriptionName position
                    |> Job.map Option.get
                let updateSubscription connection lastSeen = job {
                    use cmd =
                        Repository.prepateUpdateLastSeenSubscription connection subscription.Id lastSeen
                    do! cmd |> executeNonQueryIgnoreCt ct
                }
                //TODO Try advisory lock?
                // printfn "start"
                let mutable lastSeen = subscription.LastSeen
                let mutable init = true
                let mutable lastReceived = 0UL



                let output event = job {

                    // printfn "out start"
                    // use scope = new Transactions.TransactionScope(Transactions.TransactionScopeAsyncFlowOption.Enabled)

                    use! connection = connStr |> createOpenConnectionCt ct
                    // connection.EnlistTransaction(Transactions.Transaction.Current)
                    use transaction = connection.BeginTransaction()
                    let ack = IVar()
                    let doneAck = IVar()
                    do! Stream.Src.value output (connection,IVar.fill ack () |> memo,event, upcast doneAck )
                    do! ack
                    lastSeen <- lastSeen + (event |> Seq.length |> uint64)
                    // printfn "last seen %A" lastSeen
                    //TODO: Save subscription
                    do! updateSubscription connection lastSeen
                    // printfn "commit transaction"
                    do! transaction.CommitAsync(ct) |> Job.awaitUnitTask
                    do! IVar.fill doneAck ()
                    // scope.Complete()
                    // printfn "out end"
                }



                let catchup () = job {
                    // printfn "catchup start streamName %A lastSeen %A readLimit %A" streamName lastSeen readLimit
                    let! result = Repository.readEventsForwardBatched ct connStr streamName lastSeen readLimit
                    // printfn "catchup result: %A" result
                    match result with
                    | None -> ()
                    | Some stream ->
                        do! stream
                            // |> Stream.takeUntil (Alt.fromCT ct)
                            |> Stream.iterJob output

                    // printfn "catchup end"
                }

                //get getOrCreateSubscription
                //get any new events
                let self = { notification = Ch()}
                let notification () =
                    Ch.take self.notification
                    ^-> fun notification ->
                        if notification.StreamName = streamName then
                            //TODO: WHY -1?
                            lastReceived <- notification.LastWriteVersion

                let proc = Job.delay ^ fun () -> job {
                    if init then
                        // printfn "INIT CATCHUP lastSeen: %d / lastReceived %d " lastSeen lastReceived
                        do! catchup ()
                        init <- false
                    else if lastSeen < lastReceived then
                        // printfn "CAN CATCHUP lastSeen: %d / lastReceived %d " lastSeen lastReceived
                        do! catchup ()
                    else
                        // printfn "CAN GET NOTFICATIONS lastSeen: %d / lastReceived %d " lastSeen lastReceived
                        do! notification ()
                }

                return! Job.foreverServer proc >>-. self
            }
        let notify (subscriber : Subscriber) notification =
            Ch.give subscriber.notification notification


    let startNotify (ct : CancellationToken) connStr = job {
        // -- Payload text contains:
        //             --  * `stream_name`
        //             --  * `stream_id`
        //             --  * first `stream_version`
        //             --  * last `stream_version`
        //             -- Each separated by a comma (e.g. 'stream-12345,1,1,5')

        // Forcing KeepAlive to 30 seconds.  May need configurable/tweeked.
        // http://www.npgsql.org/doc/wait.html#keepalive
        let connStr = NpgsqlConnectionStringBuilder(connStr |> string , KeepAlive = 30)
        let output = Stream.Src.create<NpgsqlNotificationEventArgs>()
        let! conn = createOpenConnectionCt ct connStr
        use cmd = new NpgsqlCommand(sprintf "LISTEN \"%s\"" "events", conn)
        do! cmd |> executeNonQueryIgnoreCt ct

        let notificationSub =
            conn.Notification.Subscribe (Stream.Src.value output >> start)

        job {
            let cleanup = job {
                // Wait for connection to really stop fetching before closing
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
                        // Haven't seen issues swallowing this yet
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



    type Eventstore
        (connString : NpgsqlConnectionStringBuilder,
         cts : CancellationTokenSource,
         writer : Mailbox<Commands.Msg>,
         ?readLimit) =


        let appendToStream streamName version events  =
            Commands.appendToStream streamName version events  writer
        let linkToStream streamName version eventIds =
            Commands.linkToStream streamName version eventIds writer
        let linkToStreamTransaction conn doneAck streamName version eventIds =
            Commands.linkToStreamTransaction conn doneAck streamName version eventIds writer
        let linkToStreamTransactionBatch  conn doneAck batch =
            Commands.linkToStreamTransactionBatch conn doneAck batch writer
        let readLimit = defaultArg readLimit 1000UL
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

        member __.LinkToStreamTransaction =
            linkToStreamTransaction

        member __.LinkToStreamTransactionBatch =
            linkToStreamTransactionBatch
        member __.GetStreamInfo steamName = job {
            use! conn =  connString |> DbHelpers.createOpenConnectionCt cts.Token
            return! Repository.getStreamByName cts.Token conn steamName
        }

        member __.GetStreamInfo2 conn steamName = job {
            // use! conn =  connString |> DbHelpers.createOpenConnectionCt cts.Token
            return! Repository.getStreamByName cts.Token conn steamName
        }

        member __.ReadFowardFromStream =
            readStreamFoward

        member __.SubscribeToStream streamName subscriptionName (subscriptionPosition : SubscriptionPosition)= job {
            let src = Stream.Src.create<IVar<unit>*DbTypes.RecordedEvent>()
            return Stream.Src.tap src
        }
        member __.UnsubscribeToStream streamName subscriptionName (subscriptionPosition : SubscriptionPosition)= job {
            ()
        }

        static member Create (connString : NpgsqlConnectionStringBuilder) = job {
            let cts = new CancellationTokenSource()
            let! writer = Commands.create cts.Token connString
            return new Eventstore(connString,cts,writer)
        }

        interface IDisposable with
            member __.Dispose() = __.Dispose()
