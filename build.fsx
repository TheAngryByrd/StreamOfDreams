#r @"packages/build/FAKE/tools/FakeLib.dll"

#load "./lib/build/ProcessHelper.fsx"

open Fake
open Fake.Git
open Fake.AssemblyInfoFile
open Fake.ReleaseNotesHelper
open Fake.UserInputHelper
open System
open BDS.BuildTools
open BDS.BuildTools.ProcessHelper

let release = LoadReleaseNotes "RELEASE_NOTES.md"

let sln = "StreamOfDreams.sln"
let srcGlob = "src/**/*.fsproj"
let testsGlob = "tests/**/*.fsproj"

let (^) f x = f x

Target "Clean" (fun _ ->
    ["bin"; "temp" ;"dist"]
    |> CleanDirs

    !! srcGlob
    ++ testsGlob
    |> Seq.collect(fun p ->
        ["bin";"obj"]
        |> Seq.map(fun sp ->
             IO.Path.GetDirectoryName p @@ sp)
        )
    |> CleanDirs

    )

Target "LoggingFile" (fun _ ->
    ReplaceInFiles [ "namespace Logary.Facade", "namespace StreamOfDreams.Logging" ]
                   [ "paket-files/logary/logary/src/Logary.Facade/Facade.fs" ]
)

Target "DotnetRestore" ^ fun _ ->
        DotNetCli.Restore ^ fun c ->
            { c with
                Project = sln
                //This makes sure that Proj2 references the correct version of Proj1
                AdditionalArgs = [sprintf "/p:PackageVersion=%s" release.NugetVersion]
            }

Target "DotnetBuild" ^ fun _ ->
    DotNetCli.Build ^ fun c ->
        { c with
            Project = sln
            //This makes sure that Proj2 references the correct version of Proj1
            AdditionalArgs = [sprintf "/p:PackageVersion=%s" release.NugetVersion]
        }


let invoke f = f ()
let invokeAsync f = async { f () }

type TargetFramework =
| Full of string
| Core of string

let (|StartsWith|_|) prefix (s: string) =
    if s.StartsWith prefix then Some () else None

let getTargetFramework tf =
    match tf with
    | StartsWith "net4" -> Full tf
    | StartsWith "netcoreapp" -> Core tf
    | _ -> failwithf "Unknown TargetFramework %s" tf

let getTargetFrameworksFromProjectFile (projFile : string)=
    let doc = Xml.XmlDocument()
    doc.Load(projFile)
    doc.GetElementsByTagName("TargetFrameworks").[0].InnerText.Split(';')
    |> Seq.map getTargetFramework
    |> Seq.toList

let selectRunnerForFramework tf =
    let runMono = sprintf "mono -f %s -c Release --loggerlevel Warn"
    let runCore = sprintf "run -f %s -c Release"
    match tf with
    | Full t when isMono-> runMono t
    | Full t -> runCore t
    | Core t -> runCore t


let runTests modifyArgs =
    !! testsGlob
    |> Seq.map ^ fun proj ->
        proj, getTargetFrameworksFromProjectFile proj
    |> Seq.collect ^ fun (proj, targetFrameworks) ->
        targetFrameworks
        |> Seq.map selectRunnerForFramework
        |> Seq.map ^ fun args -> fun () ->
            DotNetCli.RunCommand (fun c ->
            { c with
                WorkingDir = IO.Path.GetDirectoryName proj
            }) (modifyArgs args)



Target "DotnetTestSolo" ^ fun _ ->
    runTests (sprintf "%s")
    |> Seq.iter invoke

Target "DotnetTest" ^ fun _ ->
    runTests (sprintf "%s --no-build")
    |> Seq.iter invoke


let execProcAndReturnMessages filename args =
    let args' = args |> String.concat " "
    ProcessHelper.ExecProcessAndReturnMessages
                (fun psi ->
                    psi.FileName <- filename
                    psi.Arguments <-args'
                ) (TimeSpan.FromMinutes(1.))

let pkill args =
    execProcAndReturnMessages "pkill" args

let killParentsAndChildren processId=
    pkill [sprintf "-P %d" processId]


Target "WatchTests" (fun _ ->
    runTests (sprintf "watch %s --no-restore")
    |> Seq.iter (invokeAsync >> Async.Catch >> Async.Ignore >> Async.Start)

    printfn "Press enter to stop..."
    Console.ReadLine() |> ignore

)

Target "DotnetPack" ^ fun _ ->
    !! srcGlob
    |> Seq.iter ^ fun proj ->
        DotNetCli.Pack ^ fun c ->
            { c with
                Project = proj
                Configuration = "Release"
                OutputPath = IO.Directory.GetCurrentDirectory() @@ "dist"
                AdditionalArgs =
                    [
                        sprintf "/p:PackageVersion=%s" release.NugetVersion
                        sprintf "/p:PackageReleaseNotes=\"%s\"" (String.Join("\n",release.Notes))
                        "/p:SourceLinkCreate=true"
                    ]
            }



Target "Publish" ^ fun _ ->
    Paket.Push ^ fun c ->
            { c with
                PublishUrl = "https://www.nuget.org"
                WorkingDir = "dist"
            }

Target "Release" ^ fun _ ->

    if Git.Information.getBranchName "" <> "master" then failwith "Not on master"

    StageAll ""
    Git.Commit.Commit "" (sprintf "Bump version to %s" release.NugetVersion)
    Branches.push ""

    Branches.tag "" release.NugetVersion
    Branches.pushTag "" "origin" release.NugetVersion


FinalTarget "KillStarted" <| fun _ ->
    Fake.ProcessHelper.startedProcesses
    |> Seq.map fst
    |> Seq.iter(killChildrenAndProcess (TimeSpan.FromSeconds(20.)))

ActivateFinalTarget "KillStarted"



// "Clean"
//   ==> "DotnetRestore"
"LoggingFile"
  ==> "DotnetBuild"
  ==> "DotnetTest"
  ==> "DotnetPack"
  ==> "Publish"
  ==> "Release"

"LoggingFile"
 ==> "DotnetRestore"
 ==> "WatchTests"

RunTargetOrDefault "DotnetPack"
