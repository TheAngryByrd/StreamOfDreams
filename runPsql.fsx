
#if MONO
// prevent incorrect output encoding (e.g. https://github.com/fsharp/FAKE/issues/1196)
System.Console.OutputEncoding <- System.Text.Encoding.UTF8
#endif

#r @"packages/build/FAKE/tools/FakeLib.dll"
#load "./lib/build/ProcessHelper.fsx"

open Fake
open System
open BDS.BuildTools
open BDS.BuildTools.ProcessHelper

let postgresPort = 5432

let dataDirectory  =
    __SOURCE_DIRECTORY__ @@ "data"

let postgresDataDir = dataDirectory @@ "postgres"

let killDatabases () =
    [postgresPort]
    |> Seq.map (fun x -> Fake.TraceHelper.tracefn "Killing Process on Port: %d" x ; x)
    |> Seq.iter (Shell.killProcessOnPort >> ignore)

Target "KillExistingProcesses" killDatabases


Target "Clean" <| fun _ ->
    [dataDirectory]
    |> CleanDirs


let initdbLocation = lazy (
    match Shell.which "initdb" with
    | Some path -> path
    | None ->
        if isMacOS then
            "/Applications/Postgres.app/Contents/Versions/latest/bin/initdb"
        else
            failwith "initdb unknown for this platform"
)
let initDb () =
    let exitCode =
        Fake.ProcessHelper.ExecProcess (fun psi ->
            psi.FileName <- initdbLocation.Force()
            psi.Arguments <- postgresDataDir |> sprintf "-D %s"
        ) (TimeSpan.FromSeconds(10.))
    if exitCode <> 0 then
        failwithf "initdb failed with exit code %d" exitCode


let psqlLocation = lazy (
    match Shell.which "postgres" with
    | Some path -> path
    | None ->
        if isMacOS then
            "/Applications/Postgres.app/Contents/Versions/latest/bin/postgres"
        else
            failwith "initdb unknown for this platform"
)

let psql () =
    ProcessHelper.startProcess (fun psi ->
        psi.FileName <- psqlLocation.Force()
        psi.Arguments <- postgresDataDir |> sprintf "-D %s -d 1"
    )

let waitForPortInUse port =
    let mutable portInUse = false

    while not portInUse do
        Async.Sleep(10) |> Async.RunSynchronously
        use client = new Net.Sockets.TcpClient()
        try
            client.Connect("127.0.0.1",port)
            portInUse <- client.Connected
            client.Close()
        with e ->
            client.Close()

let mutable postgresProcess = null

Target "StartPostgres" <| fun _ ->
    if not <| FileSystemHelper.fileExists (postgresDataDir @@ "postgresql.conf") then
        initDb ()
    postgresProcess <- psql ()

    waitForPortInUse postgresPort


Target "Hold" <| fun _ ->
    Console.ReadLine() |> ignore


FinalTarget "KillStarted" <| fun _ ->
    Fake.ProcessHelper.startedProcesses
    |> Seq.map fst
    |> Seq.iter(killChildrenAndProcess (TimeSpan.FromSeconds(20.)))

ActivateFinalTarget "KillStarted"

Target "Rebuild" DoNothing

// *** Define Dependencies ***
"Hold" ==> "Rebuild"
"Clean" ==> "Rebuild"
// Make sure "Clean" happens before "Hold", if "Clean" is executed during a build.
"Clean" ?=> "StartPostgres"
"Clean" ?=> "Hold"

"KillExistingProcesses" ==> "StartPostgres"

"StartPostgres" ==> "Hold"

RunTargetOrDefault "Hold"
