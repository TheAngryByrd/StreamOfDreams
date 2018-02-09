namespace BDS.BuildTools
#r @"../../packages/build/FAKE/tools/FakeLib.dll"
module ProcessHelper =
    open System
    open System.Diagnostics
    open Fake

    let setEnvVar (psi : ProcessStartInfo) (envVar : (string * string))  =
        psi.EnvironmentVariables.[fst envVar] <- snd envVar

    let setEnvVars (psi : ProcessStartInfo) (envVars : (string * string) seq)  =
        envVars
        |> Seq.iter (setEnvVar psi)


    let startProcess configProcessStartInfoF =
        let proc = new Process()
        proc.StartInfo.UseShellExecute <- false
        configProcessStartInfoF proc.StartInfo
        tracefn "%s %s" proc.StartInfo.FileName proc.StartInfo.Arguments
        Fake.ProcessHelper.start proc
        proc

    let execProcAndReturnMessages filename args =
        let args' = args |> String.concat " "
        ProcessHelper.ExecProcessAndReturnMessages (
            fun psi ->
                psi.FileName <- filename
                psi.Arguments <-args')
            (TimeSpan.FromSeconds(1.))

    module Shell =

        let private getMessages (pr : ProcessResult) = pr.Messages

        let pgrep args = execProcAndReturnMessages "pgrep" args
        let kill args  = execProcAndReturnMessages "kill"  args
        let lsof args  = execProcAndReturnMessages "lsof"  args
        let ps args  = execProcAndReturnMessages "ps"  args


        let getProcessIdByPort port =
            lsof [sprintf "-ti tcp:%d" port]
            |> getMessages
            |> Seq.tryHead
            |> Option.map int

        let getProcessesIdByPort port =
            lsof [sprintf "-ti tcp:%d" port]
            |> getMessages
            |> Seq.map int

        let processNameById pid =
            ps [sprintf "-p %d -o comm=" pid]
            |> getMessages
            |> Seq.tryHead

        let which arg =
            execProcAndReturnMessages "which" [arg]
            |> getMessages
            |> Seq.tryHead

        let whoami () =
            execProcAndReturnMessages "whoami" []
            |> getMessages
            |> Seq.head


        let killTerm  processId = kill [sprintf "-TERM %d" processId]


        let killProcessOnPort port =
            getProcessIdByPort port
            |> Option.map killTerm
        let killOnPortFilteredByNames port processNames =
            getProcessesIdByPort port
            |> Seq.map (fun pid -> (pid, pid |> processNameById))
            |> Seq.choose(fun (pid,pName) ->
                pName
                |> Option.map (fun pName -> (pid, pName.Split('/') |> Seq.last))
                )
            |> Seq.filter(fun (pid,pName) -> processNames |> Seq.contains pName)
            |> Seq.map(fst >> killTerm)


    let rec getAllChildIdsUnix parentId =
        let result = Shell.pgrep [sprintf "-P %d" parentId]

        if
            result.Messages
            |> Seq.isEmpty
        then
            [parentId]
            |> Seq.ofList
        else
            parentId
            :: (
                result.Messages
                |> Seq.toList
                // |> Seq.cache
                |> Seq.choose(
                    fun text ->
                        match Int32.TryParse(text) with
                        | (true, v) -> Some v
                        | _         -> None )
            |> Seq.collect (getAllChildIdsUnix)
            |> Seq.toList)
            |> Seq.ofList

    let killChildrenAndProcess timeout parentId =
        getAllChildIdsUnix parentId
        |> Seq.rev
        |> Seq.iter (Shell.killTerm >> ignore)
