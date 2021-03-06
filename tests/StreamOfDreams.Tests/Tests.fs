namespace StreamOfDreams.Tests


open System
open Expecto
// open Expecto.BenchmarkDotNet
open StreamOfDreams
open StreamOfDreams.DbHelpers
open Npgsql
open System.Threading
open System.Data
open System.Threading.Tasks
open Hopac
open Hopac
open Hopac.Infixes
open Newtonsoft.Json
open System.Threading.Tasks
open SimpleMigrations.DatabaseProvider
open StreamOfDreams.DbTypes
open StreamOfDreams.DbHelpers
open System.Security.Cryptography
open Expecto.Logging
open Expecto.Logging
open Hopac.Extensions
open StreamOfDreams
open System.Xml

module All =
    let (^) f x = f x


    module HopacActors =

        let tests =
            testList "BounbdedAckMB" [
                testCaseJob "Playground" <| job {
                    let! boundedAckMb = BoundedAckingMb.create 5
                    do! BoundedAckingMb.put boundedAckMb "Hello1"
                    do! BoundedAckingMb.put boundedAckMb "Hello2"
                    do! BoundedAckingMb.put boundedAckMb "Hello3"
                    do! BoundedAckingMb.put boundedAckMb "Hello4"
                    // Should be free to take one
                    let! result = BoundedAckingMb.take boundedAckMb
                    Expect.equal result "Hello1" "Not Hello1"
                    // now it's locked so we shouldn't get new value until we ack
                    let! result =
                        Alt.choose [
                            timeOutMillis 100 |> Alt.afterFun (fun _ -> "nope")
                            BoundedAckingMb.take boundedAckMb
                        ]

                    Expect.equal result "nope" "Not Hello1"
                    do! BoundedAckingMb.ack boundedAckMb
                    // Should be free
                    let! result = BoundedAckingMb.take boundedAckMb
                    Expect.equal result "Hello2" "Not Hello2"
                    // Now we need to nack again but we should be able to fill more in
                    do! BoundedAckingMb.put boundedAckMb "Hello5"
                    BoundedAckingMb.put boundedAckMb "Hello5" |> start
                    BoundedAckingMb.put boundedAckMb "Hello5" |> start
                    BoundedAckingMb.put boundedAckMb "Hello5" |> start

                    do! BoundedAckingMb.ack boundedAckMb

                    let! result = BoundedAckingMb.takeAndAck boundedAckMb

                    Expect.equal result "Hello3" "Not Hello2"
                    return ()
                }
            ]

    // module Task =

    //     let startInBackground ct (fT : unit -> Task<unit>) =
    //         Task.Factory.StartNew<_>(Func<_> (fT),ct,TaskCreationOptions.DenyChildAttach, TaskScheduler.Default).Unwrap()
    //         |> ignore


    // module Notifications =
    //     open System.Reactive
    //     open System.Threading.Tasks
    //     open System.Reactive
    //     open System.Reactive.Linq
    //     open System.Reactive.Threading.Tasks

    //     open System.Reactive.Disposables
    //     open Giraffe.Tasks

    //     let ensureOpened (conn : NpgsqlConnection) = task {
    //         if conn.State <> ConnectionState.Open then
    //             do! conn.OpenAsync()
    //     }
    //     let listenToChannel (waitTime : int) (connBuilder : NpgsqlConnectionStringBuilder) channel =
    //         Observable.Create<_>( fun (o : IObserver<_>) (ct : CancellationToken) -> task {
    //             let connBuilder = NpgsqlConnectionStringBuilder(connBuilder |> string , KeepAlive = 30)
    //             let conn = new NpgsqlConnection(connBuilder |> string)
    //             do! ensureOpened conn
    //             use cmd = new NpgsqlCommand(sprintf "LISTEN \"%s\"" channel, conn)
    //             let! _ = cmd.ExecuteNonQueryAsync()
    //             let compositeDisposable = new CompositeDisposable()
    //             compositeDisposable.Add(conn.Notification.Subscribe(o.OnNext))
    //             compositeDisposable.Add conn

    //             Task.startInBackground ct ^ fun () -> task {
    //                 //letting this run in the background because npgsql forces you to use .Wait() to receive messages
    //                 while ct.IsCancellationRequested |> not do
    //                     // do! conn.WaitAsync(ct)
    //                     do conn.Wait(waitTime) |> ignore
    //             }
    //             return Action(fun () ->
    //                 let rec spinWaitConnectionFinished count i =
    //                     if count = i || (conn.FullState.HasFlag(System.Data.ConnectionState.Fetching) |> not) then
    //                         ()
    //                     else
    //                         Thread.Sleep(i * 10)
    //                         spinWaitConnectionFinished count (i + 1)

    //                 spinWaitConnectionFinished 50 0
    //                 compositeDisposable.Dispose()
    //             )
    //         })

    // let createConnString host user pass database =
    //     let builder =
    //         sprintf "Host=%s;Username=%s;Password=%s;Database=%s" host user pass database
    //         |> NpgsqlConnectionStringBuilder
    //     builder

    // let ensureOpened (conn : NpgsqlConnection) = job {
    //     if conn.State <> ConnectionState.Open then
    //         do! conn.OpenAsync() |> Job.awaitUnitTask
    // }

    // let listenToChannel (connBuilder : NpgsqlConnectionStringBuilder) channel = job {
    //     let duplicate = NpgsqlConnectionStringBuilder(connBuilder |> string , KeepAlive = 10)
    //     let conn = new NpgsqlConnection(duplicate |> string)
    //     do! ensureOpened conn
    //     use cmd = new NpgsqlCommand(sprintf "LISTEN \"%s\"" channel, conn)
    //     let _ = cmd.ExecuteNonQueryAsync()

    //         // while true do
    //     Job.onThreadPool ^ fun _ ->
    //         conn.Wait(100) |> ignore
    //     |> Job.catch
    //     |> Job.foreverIgnore
    //     |> start
    //     return
    //         conn.Notification
    //         |> Observable.map ^ fun args ->
    //             conn,args


    // }


    // let listenDeserialize<'a> (connBuilder : NpgsqlConnectionStringBuilder) channel =
    //     listenToChannel connBuilder channel
    //     >>- Observable.map ^ fun (conn, args) -> args.AdditionalInformation |> JsonConvert.DeserializeObject<'a>


    // let sendToChannel conn channel payload = job {
    //     do! ensureOpened conn
    //     use cmd = new NpgsqlCommand(sprintf "NOTIFY \"%s\", '%s';" channel payload, conn)
    //     let! _ = cmd.ExecuteNonQueryAsync()
    //     return ()
    // }

    // let sendObjToChannel conn channel payload = job {
    //     let payload = JsonConvert.SerializeObject payload
    //     do! sendToChannel conn channel payload
    // }


    // let startwithCt ct asyncF =
    //     Async.Start(asyncF,ct)

    // let testCaseJob name xJ =
    //     xJ |> Job.toAsync |> testCaseAsync name

    // [<CLIMutable>]
    // type SomeData = {
    //     Name : string
    //     Age : int
    //     Colors : string array
    // }
    //     with
    //         static member Empty = {
    //                 Name = String.Empty
    //                 Age = 0
    //                 Colors = Array.empty }



    // let fastestWaitTime i =
    //     job {
    //             let channel = Guid.NewGuid().ToString("n")
    //             let connStr = createConnString "localhost" "jimmybyrd" "" "demo"
    //             // let! listener = listenToChannel connStr channel
    //             // let! listener = listenDeserialize<SomeData> connStr channel
    //             let sema =  new SemaphoreSlim(0)
    //             let mutable count = 0
    //             let total = 100000
    //             use sub =
    //                 Notifications.listenToChannel i connStr channel
    //                 |> Observable.subscribe ^ fun args ->
    //                     // printfn "%s - %s" args.Condition args.AdditionalInformation
    //                     count <- count + 1
    //                     if count = total then
    //                         sema.Release() |> ignore


    //             use conn2 = new NpgsqlConnection(connStr |> string)
    //             do! conn2 |> ensureOpened
    //             let dataToSend = {
    //                 Name = "Foo"
    //                 Age = 21
    //                 Colors = [|"Green";"Orange"|]
    //             }
    //             // let dts = dataToSend |> JsonConvert.SerializeObject
    //             [1..total]
    //             |> Seq.map ^ fun _ ->
    //                 dataToSend |> sendObjToChannel conn2 channel
    //                 // dts |> sendToChannel conn2 channel
    //             |> Job.seqIgnore
    //             |> start

    //             do! sema.WaitAsync() |> Job.awaitUnitTask
    //             printfn "%A" count
    //             Expect.equal count total "1000?"

    //         }

    // [<Tests>]
    // let tests =
    //     testList "listen string" [
    //         // testCaseJob "string" <| job {
    //         //     let channel = "testChannel"
    //         //     let connStr = createConnString "localhost" "jimmybyrd" "" "demo"
    //         //     let! listener = listenToChannel connStr channel
    //         //     let mutable data = null

    //         //     listener
    //         //     |> Observable.add ^ fun (conn, args) ->
    //         //         data <- args.AdditionalInformation
    //         //         printfn "obtained %s" args.AdditionalInformation

    //         //     use conn2 = new NpgsqlConnection(connStr |> string)
    //         //     conn2.Open()
    //         //     do! "helloworld" |> sendToChannel conn2 channel
    //         //     do! timeOutMillis 100
    //         //     Expect.isNotNull data "Should not be null"
    //         // }
    //         testCase "obj" <| fun () ->

    //             let result = Performance.findFastest (fastestWaitTime >> run) 1 1

    //             printfn "fastest wait time %i" result
    //     ]

    [<CLIMutable>]
    type Person = {
        Name : string
        FavoriteNumber : int
        Birthdate : DateTimeOffset
        Nicknames : string array
    }

    let inline withDatabase (f : _ -> _)  = async {
        use database = getMigratedDatabase ()
        return! f (database)
    }

    let inline withMigratedDatabase (f : _ -> _)  = async {
        use database = getMigratedDatabase ()
        return! f (database)
    }

    let helpfulConnStrAddtions =
        DatabaseTestHelpers.duplicateAndChange ^ fun cs ->
            cs.MaxAutoPrepare <- 10
            cs.Pooling <- true


    let randomDate (r : Random) =
        let start = DateTimeOffset.MinValue
        let range = (DateTimeOffset.UtcNow - start).Days;
        start.AddDays(r.Next(range) |> float)

    // TODO:  One or more errors occurred. (Out of the range of DateTime (year must be between 1 and 9999
    let generatePeopleEvents howMany =
        let r = Random(42)

        [1..howMany]
        // |> Seq.map ^ fun _ -> Guid.NewGuid()
        |> Seq.map ^ fun index ->
            {
                Name = Guid.NewGuid().ToString("n")
                FavoriteNumber = r.Next(0,100)
                Birthdate = randomDate r
                Nicknames = Array.init (r.Next(0,3)) ^ fun i -> Guid.NewGuid().ToString("n")
            }
            |> Event.AutoTypeData
        |> Seq.map ^ fun ev ->
            {RecordedEvent.Empty with EventType = ev.EventType; Data=ev.Data}

    let generateEvents howMany =
        [1..howMany]
        |> Seq.map ^ fun _ -> Guid.NewGuid()
        |> Seq.map ^ fun eventId -> {RecordedEvent.Empty with Id= eventId; EventType = "Foo"; Data="[]"}


    let eventTableTests =
        testList "Event table tests" [
            yield! testFixtureAsync withMigratedDatabase [
                testCaseJob' "Can insert records" <| fun db -> job {

                    use cts = new Disposable.CTSCancelOnDispose()
                    use connection = new NpgsqlConnection(db.Conn |> string)
                    do! connection |> ensureOpenCt cts.Token
                    let total = 9362
                    // let total = 2
                    let events =    generateEvents total
                    use cmd =
                        events
                        |> Repository.prepareInsertEvents connection

                    let! inserted = cmd |> executeReaderCt cts.Token
                    ()
                    // let legnth = inserted |> Seq.length
                    // printfn "%A" inserted
                    // Expect.equal legnth total "Didn't insert event"
                }
            ]
        ]

    let streamTableTests =
        testList "Stream Table Tests" [
            yield! testFixtureAsync withMigratedDatabase [
                testCaseJob' "No Streams inserted yet" <| fun db -> job {
                    use cts = new Disposable.CTSCancelOnDispose()
                    use! connection = createOpenConnectionCt cts.Token db.Conn
                    let! result = Repository.getStreamByName cts.Token connection "DoesntExist"
                    Expect.isNone result "Should find none"
                    return ()
                }
                testCaseJob' "Create Stream, find one" <| fun db -> job {
                    let streamName = "Bank-12345"
                    use cts = new Disposable.CTSCancelOnDispose()
                    use! connection = createOpenConnectionCt cts.Token db.Conn

                    do! Repository.prepareCreateStream connection streamName
                        |> executeReaderCt cts.Token
                        |> Job.usingJob' ^ fun reader ->
                             Job.result ()

                    let! result = Repository.getStreamByName cts.Token connection streamName
                    Expect.isSome result "Should find Some"
                    // printfn "%A" result
                    return ()
                }
            ]
        ]
    // let readForwardBenchTest limit =
    //     job {
    //             use db = getMigratedDatabase ()
    //             let connStr = helpfulConnStrAddtions db.Conn
    //             use connection = new NpgsqlConnection(connStr |> string)
    //             do! connection |> ensureOpen
    //             let streamName = "Bank-12345"
    //             do!
    //                 [0..10]
    //                 |> Seq.map ^ fun _ -> job {
    //                     let events = generatePeopleEvents 13107
    //                     do! Commands.appendToStream connStr streamName Commands.Version.Any events }
    //                 |> Job.seqIgnore
    //             let! result = Repository.getStreamByName connection streamName
    //             Expect.isSome result "Should find Some"
    //             // printfn "%A" result

    //             let! events =
    //                 //Job.benchmark "readEventsForward" ^ fun _ ->
    //                 Repository.readEventsForward connection streamName 0 limit
    //             let events = Option.get events
    //             let! length =
    //                   Job.benchmark "readEventsForwardReal" ^ fun _ ->
    //                     events
    //                     |> Stream.foldFun (fun state item -> state + 1) 0
    //             printfn "records %A" length
    //     }

    // module Benchmarks =
    //     open BenchmarkDotNet.Attributes
    //     open BenchmarkDotNet.Diagnosers
    //     open BenchmarkDotNet.Configs
    //     open BenchmarkDotNet.Jobs
    //     open BenchmarkDotNet.Running
    //     open BenchmarkDotNet.Validators
    //     open BenchmarkDotNet.Environments


    //     type Benchy () =
    //         [<Params(100,1000,10000,100000)>]
    //         member val public limit = 0 with get, set

    //         [<Benchmark>]
    //         member x.StreamRead() =
    //             readForwardBenchTest x.limit |> run



    // let CommandPerfTest =
    //     testList "Perf" [
    //        benchmark<Benchmarks.Benchy> benchmarkConfig (ignore >> obj) |> unbox
    //     ]
        // ftestList "Perf finder" [
        //     testCaseJob "Best foward stream limit" <| job {
        //     do!
        //         [1;100;1000;5000;10000;50000;100000]
        //         |> Seq.collect ^ fun limit ->
        //             [0..3] |> Seq.map ^ fun _ ->
        //                 Job.benchmark "LIMIT BREAK" ^ fun _ -> job {
        //                     do! readForwardBenchTest limit
        //                     printfn "limit %A" limit
        //                 }
        //         |> Job.seqIgnore

            //  let foo = Performance.findFastest  1 50000
            //  printfn "Perf test, best limit %A" foo
            //  ()
            // }
        // ]

    let commandTest =
        testList "Command tests" [
            yield! testFixtureAsync withMigratedDatabase [
                testCaseJob' "Append many to single stream" <| fun db -> job {
                    use cts = new Disposable.CTSCancelOnDispose()

                    let connStr = helpfulConnStrAddtions db.Conn
                    use! connection = createOpenConnectionCt cts.Token connStr

                    let! writer = Commands.create cts.Token connStr
                    use connection = new NpgsqlConnection(connStr |> string)
                    do! connection |> ensureOpenCt cts.Token
                    let streamName = "Bank-12456"
                    let eventsToGenerate = 13
                    let! writeResult =
                        let events = generatePeopleEvents eventsToGenerate |> Seq.toArray
                        Commands.appendToStream (streamName) DomainTypes.Version.Any events writer

                    Expect.equal writeResult.NextExpectedVersion (uint64 eventsToGenerate) "Not expected Version"

                    let! allStream = Repository.getStreamByName cts.Token  connection "$all" |> Job.map Option.get
                    Expect.equal allStream.Version (uint64 eventsToGenerate) "Not expected Version"

                    let! stream = Repository.getStreamByName cts.Token  connection (streamName) |> Job.map Option.get
                    Expect.equal stream.Version (uint64 eventsToGenerate) "Not expected Version"

                    let! events =
                         Repository.readEventsForward cts.Token connStr streamName 0UL 10000UL
                         |> Job.map Option.get

                    let! length =
                            events
                            |> Stream.foldFun (fun x s -> x + 1) 0
                    Expect.equal length (eventsToGenerate) "Events not written to stream"
                    ()
                }
                testCaseJob' "Read forever" <| fun db -> job {
                    use cts = new Disposable.CTSCancelOnDispose()

                    let connStr = helpfulConnStrAddtions db.Conn
                    use! connection = createOpenConnectionCt cts.Token connStr
                    let! writer = Commands.create cts.Token connStr
                    use connection = new NpgsqlConnection(connStr |> string)
                    do! connection |> ensureOpenCt cts.Token
                    let streamName = "Bank-12456"
                    let eventsToGenerate = 13
                    let! writeResult =
                        let events = generatePeopleEvents eventsToGenerate |> Seq.toArray
                        Commands.appendToStream (streamName) DomainTypes.Version.Any events writer

                    Expect.equal writeResult.NextExpectedVersion (uint64 eventsToGenerate) "Not expected Version"

                    let! allStream = Repository.getStreamByName cts.Token connection "$all" |> Job.map Option.get
                    Expect.equal allStream.Version (uint64 eventsToGenerate) "Not expected Version"

                    let! stream = Repository.getStreamByName cts.Token connection (streamName) |> Job.map Option.get
                    Expect.equal stream.Version (uint64 eventsToGenerate) "Not expected Version"

                    let! events =
                         Repository.readEventsForwardForever cts.Token connStr streamName 0UL 10000UL
                         |> Job.map Option.get
                    let mutable length = 0UL
                    let sema =  new SemaphoreSlim(0)
                    events
                    |> Stream.iterJob(fun _ -> job {
                        length <- length + 1UL
                        if length = writeResult.NextExpectedVersion then
                            sema.Release() |>ignore
                            return! Job.abort()
                    })
                    |> start
                    do! sema.WaitAsync() |> Job.awaitUnitTask
                    Expect.equal length ((writeResult.NextExpectedVersion)) "Events not written to stream"
                    let nextGen = 1000
                    let! writeResult =
                        let events = generatePeopleEvents nextGen |> Seq.toArray
                        Commands.appendToStream (streamName) DomainTypes.Version.Any events writer
                    events
                    |> Stream.iterJob(fun _ -> job {
                        length <- length + 1UL
                        if length = writeResult.NextExpectedVersion then
                            sema.Release() |>ignore
                            return! Job.abort()
                    })
                    |> start
                    do! sema.WaitAsync() |> Job.awaitUnitTask
                    Expect.equal (length) (writeResult.NextExpectedVersion) "Events not written to stream"
                }

                testCaseJob' "Stream exists, should fail with NoStream set" <| fun db -> job {
                    use cts = new Disposable.CTSCancelOnDispose()

                    let connStr = helpfulConnStrAddtions db.Conn
                    use! connection = createOpenConnectionCt cts.Token connStr

                    let! writer = Commands.create cts.Token connStr
                    let streamName = "Bank-124562"

                    do! Repository.prepareCreateStream  connection streamName |> executeNonQueryIgnoreCt cts.Token
                    let events = generatePeopleEvents 1 |> Seq.toArray
                    do! Expect.throwsTJ<Commands.ConcurrencyException> (fun () ->Commands.appendToStream streamName DomainTypes.Version.NoStream events writer |> Job.Ignore ) "Should throw concurreny exception"
                    ()
                }
                testCaseJob' "Stream doesnt exist, should fail with StreamExists set" <| fun db -> job {
                    use cts = new Disposable.CTSCancelOnDispose()

                    let connStr = helpfulConnStrAddtions db.Conn

                    let! writer = Commands.create cts.Token connStr

                    let streamName = "Bank-124562"

                    let events = generatePeopleEvents 1 |> Seq.toArray
                    do! Expect.throwsTJ<Commands.ConcurrencyException> (fun () ->Commands.appendToStream streamName DomainTypes.Version.StreamExists events writer |> Job.Ignore ) "Should throw concurreny exception"
                    ()
                }
                testCaseJob' "Stream exist with items already, should fail with EmptyStream set" <| fun db -> job {
                    use cts = new Disposable.CTSCancelOnDispose()

                    let connStr = helpfulConnStrAddtions db.Conn

                    let! writer = Commands.create cts.Token connStr

                    let streamName = "Bank-124562"

                    let events = generatePeopleEvents 1 |> Seq.toArray
                    do! Commands.appendToStream streamName DomainTypes.Version.Any events writer |> Job.Ignore

                    let events = generatePeopleEvents 1 |> Seq.toArray
                    do! Expect.throwsTJ<Commands.ConcurrencyException> (fun () ->Commands.appendToStream streamName DomainTypes.Version.EmptyStream events writer |> Job.Ignore ) "Should throw concurreny exception"
                    ()
                }
                testCaseJob' "Stream doesn't exist, should fail with EmptyStream set" <| fun db -> job {
                    use cts = new Disposable.CTSCancelOnDispose()

                    let connStr = helpfulConnStrAddtions db.Conn

                    let! writer = Commands.create cts.Token connStr

                    let streamName = "Bank-124562"

                    let events = generatePeopleEvents 1 |> Seq.toArray
                    do! Expect.throwsTJ<Commands.ConcurrencyException> (fun () ->Commands.appendToStream streamName DomainTypes.Version.EmptyStream events writer |> Job.Ignore ) "Should throw concurreny exception"
                    ()
                }
                testCaseJob' "Stream exists with another version, should fail with Version value set to another number" <| fun db -> job {
                    use cts = new Disposable.CTSCancelOnDispose()

                    let connStr = helpfulConnStrAddtions db.Conn

                    let! writer = Commands.create cts.Token connStr
                    let streamName = "Bank-124562"

                    let events = generatePeopleEvents 3 |> Seq.toArray
                    do! Commands.appendToStream streamName DomainTypes.Version.Any events writer |> Job.Ignore

                    let events = generatePeopleEvents 1 |> Seq.toArray
                    do! Expect.throwsTJ<Commands.ConcurrencyException> (fun () ->Commands.appendToStream streamName (DomainTypes.Version.Value 1UL) events writer |> Job.Ignore ) "Should throw concurreny exception"
                    ()
                }
                testCaseJob' "Stream doesnt exist, should succeed with Version value set to 0" <| fun db -> job {
                    use cts = new Disposable.CTSCancelOnDispose()

                    let connStr = helpfulConnStrAddtions db.Conn
                    let! writer = Commands.create cts.Token connStr

                    let streamName = "Bank-124562"

                    let events = generatePeopleEvents 1 |> Seq.toArray
                    do! Commands.appendToStream streamName (DomainTypes.Version.Value 0UL) events writer |> Job.Ignore

                }
                testCaseJob' "Stream doesnt exist, should fail with Version value set to number" <| fun db -> job {
                    use cts = new Disposable.CTSCancelOnDispose()

                    let connStr = helpfulConnStrAddtions db.Conn

                    let! writer = Commands.create cts.Token connStr

                    let streamName = "Bank-124562"

                    let events = generatePeopleEvents 1 |> Seq.toArray
                    do! Expect.throwsTJ<Commands.ConcurrencyException> (fun () ->Commands.appendToStream streamName (DomainTypes.Version.Value 1UL) events writer |> Job.Ignore ) "Should throw concurreny exception"
                    ()
                }
            ]
        ]

    let createByEventType (event : RecordedEvent) = String.format "$et-{0}" event.EventType
    // let eventTypeProjection (eventstore : Eventstore.Eventstore) ct  (connStr) = job {

    //     let! notifications = StreamOfDreams.Subscriptions.startNotify ct connStr

    //     notifications
    //     |> Stream.filterFun(fun n -> n.StreamName = "$all")
    //     // |> Stream.takeUntil (Alt.fromCT ct)
    //     |> Stream.iterJob (fun notification -> job {
    //         let diff  = (notification.LastWriteVersion - notification.FirstWriteVersion)
    //         let! events =
    //             StreamOfDreams.Repository.readEventsForward ct connStr notification.StreamName (notification.FirstWriteVersion ) diff
    //             |> Job.map Option.get
    //             |> Job.map (Stream.take (int64 diff))
    //             |> Job.bind(Stream.toSeq)
    //         // printfn "%A" notification
    //         // printfn "%A" events
    //         // use conn = builderToConnection connStr
    //         // do! ensureOpen conn
    //         do!
    //                 events
    //                 |> Seq.groupBy(fun e -> e.EventType)
    //                 |> Seq.map(fun (e, xs) -> job {
    //                     let streamName = createByEventType e

    //                     //TODO: keep track of version
    //                     let! streamInfo = eventstore.GetStreamInfo streamName
    //                     let version =
    //                         match streamInfo with
    //                         | Some s -> s.Version
    //                         | None -> 0UL
    //                     let! result =
    //                         xs
    //                         |> Seq.map(fun e -> e.Id)
    //                         |> eventstore.LinkToStream streamName (DomainTypes.Version.Value version)
    //                     ()


    //                 })
    //                 |> Job.seqIgnore
    //         // printfn "%A" events
    //     })
    //     |> start
    //     ()
    // }

    let notifyTests =
        testList "Notify tests" [
            yield testCase "Can parse normal notification string" <| fun () ->
                let notificationStr = "Bank-124562,1,1,3"
                let (expected : Subscriptions.Notification) = {
                    StreamName = "Bank-124562"
                    StreamId   = 1UL
                    LastVersion = 1UL
                    LastWriteVersion = 3UL
                }
                let actual =  Subscriptions.Notification.parse notificationStr
                Expect.equal actual expected "Didn't parse notification event correctly"
            yield testCase "Can parse devious notification string" <| fun () ->
                let notificationStr = "Bank,124562,1,1,3"
                let (expected : Subscriptions.Notification) = {
                    StreamName = "Bank,124562"
                    StreamId   = 1UL
                    LastVersion = 1UL
                    LastWriteVersion = 3UL
                }

                let actual =  Subscriptions.Notification.parse notificationStr
                Expect.equal actual expected "Didn't parse notification event correctly"

            yield! testFixtureAsync withMigratedDatabase [
                testCaseJob' "Subscriber no events yet" <| fun db -> job {
                    let random = Random(42)
                    use cts = new Disposable.CTSCancelOnDispose()
                    let ct = cts.Token
                    let connStr = db.Conn |> helpfulConnStrAddtions
                    let! notifier =
                        StreamOfDreams.Subscriptions.startNotify ct connStr

                    let src = Stream.Src.create()

                    use! eventstore =  Eventstore.Eventstore.Create(db.Conn)

                    let! subscriber =
                        Subscriptions.Subsciption.create
                            ct
                            connStr
                            "$all"
                            "GET ALL"
                            DomainTypes.SubscriptionPosition.Continue
                            src
                            13100UL

                    let streamName () =

                         sprintf "Bank-%d" (random.Next(0,5))
                    let eventCount = 100

                    let events = generatePeopleEvents eventCount |> Seq.toArray
                    do! eventstore.AppendToStream (streamName ()) DomainTypes.Version.Any events |> Job.Ignore
                    // do! Commands.appendToStream streamName DomainTypes.Version.Any events writer
                    //     |> Job.Ignore
                    let outputStream = Stream.Src.tap src
                    do! timeOutMillis 100
                    let! notifyingDone =
                        notifier
                        // |> Stream.mapFun(fun x -> printfn "NOTIFICATION %A" x; x)
                        |> Stream.iterFun(Subscriptions.Subsciption.notify subscriber >> start)
                        |> Promise.start
                    let! outputDone =
                         outputStream
                        //  |> Stream.takeUntil (Alt.fromCT cts.Token)
                         |> Stream.iterJob ^ fun (conn, ack, event, doneAck) -> job {


                            let! byEventTypes =
                                event
                                |> Seq.groupBy(fun e -> e.EventType)
                                |> Seq.map(fun (eventType, events) -> job {
                                    let streamName = events |> Seq.head |> createByEventType
                                    // printfn "streamname %A" streamName
                                    let! streamInfo = eventstore.GetStreamInfo2 conn streamName
                                    let version =
                                        match streamInfo with
                                        | Some s -> s.Version
                                        | None -> 0UL

                                    return (streamName, (DomainTypes.Version.Value version), events |> Seq.map(fun e -> e.Id))

                                })
                                |> Job.seqCollect

                            let! result =
                                byEventTypes
                                :> seq<_>
                                |> eventstore.LinkToStreamTransactionBatch conn doneAck
                            // printfn "result: %A" result
                            do! ack
                            // return result
                            // let streamName = event|> createByEventType
                            // let! streamInfo = eventstore.GetStreamInfo2 conn streamName
                            // let version =
                            //     match streamInfo with
                            //     | Some s -> s.Version
                            //     | None -> 0UL


                            // let! result =
                            //     [event]
                            //     |> Seq.map(fun e -> e.Id)
                            //     |> eventstore.LinkToStreamTransaction conn doneAck streamName (DomainTypes.Version.Value version)
                            // printfn "result: %A" result
                            // do! ack

                            // TODO: keep track of version
                            // let! results =
                            //     byEventTypes
                            //     |> Job.seqCollect

                            // let! result =
                            //     [|event.Id|]
                            //     |> Array.toSeq
                            //     |> eventstore.LinkToStreamTransaction conn doneAck streamName (DomainTypes.Version.Value version)
                            // printfn "result %A" result

                            return ()
                         }
                         |> Promise.start

                    let times = 10
                    // do! timeOutMillis 1000
                    let eventCount2 = 13106

                    let finished = uint64 (eventCount + (times * eventCount2) - 1)
                    for i in 1..times do
                        let events = generatePeopleEvents eventCount2 |> Seq.toArray

                        eventstore.AppendToStream (streamName ()) DomainTypes.Version.Any events
                            |> Job.Ignore
                            |> start

                    // do! timeOutMillis 3000
                    let! stream =
                        StreamOfDreams.Repository.readEventsForwardForever cts.Token connStr "$et-person" 0UL 1000UL
                        |> Job.map Option.get
                    do!
                        stream
                        |> Stream.takeWhileFun(fun (x : RecordedEvent) ->
                            // printfn "sn %A" x.StreamVersion
                            x.StreamVersion < finished)
                        |> Stream.iterFun (ignore )

                    do! timeOutMillis 100
                    let! streamInfo = eventstore.GetStreamInfo "$et-person"
                    let streamInfo = streamInfo |> Option.get
                    printfn "%A" streamInfo
                    // printfn "waiting"`
                    // do! timeOutMillis 100000
                    cts.Dispose()
                    do! timeOutMillis 100
                    printfn "Console enter"
                    Console.ReadLine() |> ignore
                    // do! notifyingDone
                    // do! outputDone
                    // printfn "GCing"
                    // GC.Collect()
                    // GC.WaitForPendingFinalizers()

                    // printfn "GCing complete"
                    // do! timeOutMillis 10000
                    return ()
                }
                // testCaseJob' "Foo" <| fun db -> job {
                //     use cts = new CancellationTokenSource()
                //     // let foo = db.Conn
                //     let! notificationChannel = StreamOfDreams.Subscriptions.startNotify cts.Token db.Conn
                //     let! writer = Commands.create db.Conn
                //     use connection = builderToConnection db.Conn
                //     do! connection |> ensureOpen
                //     let streamName = "Bank-124562"
                //     let eventCount = 3
                //     let events = generatePeopleEvents eventCount |> Seq.toArray
                //     do! Commands.appendToStream streamName DomainTypes.Version.Any events writer
                //         |> Job.Ignore

                //     let! promise =
                //         notificationChannel
                //         |> Stream.toSeq
                //         |> Promise.start

                //     do! Commands.appendToStream streamName DomainTypes.Version.Any events writer
                //         |> Job.Ignore
                //     let expected =
                //         [
                //             "Bank-124562,1,0,3"
                //             "$all,0,0,3"
                //             "Bank-124562,1,3,6"
                //             "$all,0,3,6"
                //         ]
                //         |> Seq.map Subscriptions.Notification.parse

                //     cts.Cancel()
                //     let! result = promise
                //     Expect.sequenceEqual result expected "Did not receive notification events in order"
                // }
                // testCaseJob' "foo2" <| fun db -> job {
                //     use! eventstore = Eventstore.Eventstore.Create(db.Conn)
                //     use cts = new Disposable.CTSCancelOnDispose()
                //     use conn = db.Conn |> builderToConnection
                //     do! ensureOpen conn
                //     do! eventTypeProjection eventstore cts.Token  db.Conn
                //     let streamName = "Bank-124562"
                //     let eventCount = 100
                //     let! writer = Commands.create db.Conn
                //     let events = generatePeopleEvents eventCount |> Seq.toArray

                //     let upTo = 20
                //     do! [1..upTo]
                //         |> Seq.map (fun _ ->
                //             eventstore.AppendToStream streamName DomainTypes.Version.Any events |> Job.Ignore)
                //         |> Job.seqIgnore

                //     //TODO: Waits are bad.  You should feel bad.
                //     do! timeOutMillis 1000

                //     let! stream =
                //         Repository.getStreamByName conn "$et-person"
                //         |> Job.map Option.get
                //     // printfn "%A" stream
                //     Expect.equal stream.Version (uint64 eventCount * uint64 upTo) "Not same"

                // }
            ]
        ]

    [<Tests>]
    let tests =
        testList
            "All"
            [
                yield! testFixtureAsync withDatabase [
                        testCaseAsync' "Can Migrate Up and Down" <| fun db -> async {
                            let connStr =  db.Conn |> DatabaseTestHelpers.duplicateAndChange (fun cs -> cs.MaxAutoPrepare <- 10)
                            use connection = new NpgsqlConnection(connStr |> string)

                            let databaseProvider = new PostgresqlDatabaseProvider(connection)
                            let migrator = new SimpleMigrations.SimpleMigrator(typedefof<StreamOfDreams.DbTypes.Event>.Assembly, databaseProvider)
                            migrator.Load()
                            migrator.MigrateToLatest()
                            migrator.MigrateTo(0L)
                        }
                ]
                yield eventTableTests
                yield streamTableTests
                yield commandTest
                yield HopacActors.tests
                yield notifyTests
                // yield CommandPerfTest
            ]
