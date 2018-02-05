namespace StreamOfDreams.Tests


open System
open Expecto
// open Expecto.BenchmarkDotNet
open StreamOfDreams
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

module All =
    let (^) f x = f x

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
                    use connection = new NpgsqlConnection(db.Conn |> string)
                    do! connection |> ensureOpen
                    let total = 9362
                    // let total = 2
                    let events =    generateEvents total
                    use cmd =
                        events
                        |> Repository.prepareInsertEvents connection

                    let! inserted = cmd |> executeReader
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
                    use connection = new NpgsqlConnection(db.Conn |> string)
                    do! connection |> ensureOpen
                    let! result = Repository.getStreamByName connection "DoesntExist"
                    Expect.isNone result "Should find none"
                    return ()
                }
                testCaseJob' "Create Stream, find one" <| fun db -> job {
                    let streamName = "Bank-12345"
                    use connection = new NpgsqlConnection(db.Conn |> string)
                    do! connection |> ensureOpen

                    do! Repository.prepareCreateStream connection streamName
                        |> executeReader
                        |> Job.usingJob' ^ fun reader ->
                             Job.result ()

                    let! result = Repository.getStreamByName connection streamName
                    Expect.isSome result "Should find Some"
                    // printfn "%A" result
                    return ()
                }
            ]
        ]
    let readForwardBenchTest limit =
        job {
                use db = getMigratedDatabase ()
                let connStr = helpfulConnStrAddtions db.Conn
                use connection = new NpgsqlConnection(connStr |> string)
                do! connection |> ensureOpen
                let streamName = "Bank-12345"
                do!
                    [0..10]
                    |> Seq.map ^ fun _ -> job {
                        let events = generatePeopleEvents 13107
                        do! Commands.appendToStream connStr streamName Commands.Version.Any events }
                    |> Job.seqIgnore
                let! result = Repository.getStreamByName connection streamName
                Expect.isSome result "Should find Some"
                // printfn "%A" result

                let! events =
                    //Job.benchmark "readEventsForward" ^ fun _ ->
                    Repository.readEventsForward connection streamName 0 limit
                let events = Option.get events
                let! length =
                      Job.benchmark "readEventsForwardReal" ^ fun _ ->
                        events
                        |> Stream.foldFun (fun state item -> state + 1) 0
                printfn "records %A" length
        }

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
                testCaseJob' "Append test" <| fun db -> job {
                    let connStr = helpfulConnStrAddtions db.Conn
                    use connection = new NpgsqlConnection(connStr |> string)
                    do! connection |> ensureOpen
                    let streamName = "Bank-12345"
                    do!
                        [0..0]
                        |> Seq.map ^ fun _ -> job {
                            let events = generatePeopleEvents 13107
                            do! Commands.appendToStream connStr streamName Commands.Version.Any events }
                        |> Job.seqIgnore
                    let! result = Repository.getStreamByName connection streamName
                    Expect.isSome result "Should find Some"
                    printfn "%A" result

                    let! events =
                        Job.benchmark "readEventsForward" ^ fun _ -> Repository.readEventsForward connection streamName 0 1000
                    let events = Option.get events
                    let! length =
                          Job.benchmark "readEventsForwardReal" ^ fun _ ->
                            events
                            |> Stream.mapFun (fun x -> x.Data |> Json.deserialize<Person>)
                            |> Stream.foldFun (fun x s -> x + 1) 0
                    printfn "records %A" length

                }
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
                // yield CommandPerfTest
            ]
