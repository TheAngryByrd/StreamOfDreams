module ExpectoTemplate
open Expecto

[<EntryPoint>]
let main argv =
    { StreamOfDreams.Logging.Global.defaultConfig
        with
            getLogger        = fun name -> StreamOfDreams.Logging.LiterateConsoleTarget(name,  StreamOfDreams.Logging.Verbose) :>  StreamOfDreams.Logging.Logger
    }
    |> StreamOfDreams.Logging.Global.initialise
    Tests.runTestsInAssembly defaultConfig argv
