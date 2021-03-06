namespace StreamOfDreams

[<AutoOpen>]
module Infixes =
    let (^) x = (<|) x


module Ex =
    open System
    open System.Runtime.ExceptionServices
    open System.Reflection
    /// Capture exception (.NET 4.5+), keep the stack, add current stack.
    /// This puts the origin point of the exception on top of the stacktrace.
    /// It also adds a line in the trace:
    /// "--- End of stack trace from previous location where exception was thrown ---"
    let inline throwCapture ex =
        ExceptionDispatchInfo.Capture(ex).Throw()
        failwith "Unreachable code reached."

    /// Modify the exception, preserve the stacktrace and add the current stack, then throw (.NET 2.0+).
    /// This puts the origin point of the exception on top of the stacktrace.
    let inline throwPreserve ex =
        // let food = BindingFlags.Instance ||| BindingFlags.NonPublic
        let preserveStackTrace =
            typeof<Exception>.GetMethod("InternalPreserveStackTrace", BindingFlags.Instance ||| BindingFlags.NonPublic)

        (ex, null)
        |> preserveStackTrace.Invoke  // alters the exn, preserves its stacktrace
        |> ignore

        raise ex
    /// Wrap the exception, this will put the Core.Raise on top of the stacktrace.
    /// This puts the origin of the exception somewhere in the middle when printed, or nested in the exception hierarchy.
    let inline throwWrapped ex =
        exn("Oops", ex)
        |> raise


module Hopac =
    open Hopac
    open Hopac.Infixes
    open System.Threading
    open System.Collections.Generic

    module Job =
        let inline teeJob (xJ : 'a -> Job<unit>) x =
            xJ x
            >>-. x


        let inline usingJob (xJ : Job<'a>) (xJyJ : 'a -> Job<'b>)=
            xJ
            |> Job.bind ^ fun dis ->
                Job.using dis xJyJ

        let inline usingJob' (xJyJ : 'a -> Job<'b>) (xJ : Job<'a>) =
            usingJob xJ xJyJ

        let inline benchmark name (f : unit -> Job<'a>) = job {
            let sw = System.Diagnostics.Stopwatch.StartNew()
            let! result = f ()
            sw.Stop()
            printfn "%s took %A ms" name sw.ElapsedMilliseconds
            return result
        }

    module Alt =
        let fromCT (ct : CancellationToken) =
            let cancelled = IVar()
            let sub = ct.Register(fun () ->
                cancelled *<=  () |> start)
            (cancelled)
            ^-> sub.Dispose


    type BoundedAckingMb<'x> = {putCh: Ch<'x>; takeCh: Ch<'x>; ackCh : Ch<unit>}

    module BoundedAckingMb =
        type State =
        | NeedAck
        | Free

        let create capacity = Job.delay <| fun () ->
            let mutable state = Free
            let self = {putCh = Ch (); takeCh = Ch (); ackCh = Ch ()}
            let queue = Queue<_>()
            let put () = self.putCh ^-> queue.Enqueue
            let take () =
                self.takeCh *<- queue.Peek () ^-> fun _ -> state <- NeedAck
            let ack () =
                self.ackCh *<- () ^-> (queue.Dequeue >> fun _ -> state <- Free)
            let proc = Job.delay <| fun () ->
                match queue.Count, state with
                | 0, _ ->
                    put ()
                | n, Free when n = capacity ->
                    take ()
                | n, NeedAck when n = capacity ->
                    ack()
                | _, Free ->
                    take () <|> put ()
                | _, NeedAck ->
                    ack () <|> put ()
            Job.foreverServer proc >>-. self
        let put xB x = xB.putCh *<- x
        let take xB = xB.takeCh :> Alt<_>
        let ack xB = xB.ackCh :> Alt<_>
        let takeAndAck xB =
            take xB ^=>
                (fun x -> job {
                    do! ack xB
                    return x })

    module Actors =
        type Actor<'msg> = Mailbox<'msg>
        type ReplyChannel<'a> = IVar<'a>
        let actor (body: Actor<'msg> -> Job<unit>) : Job<Mailbox<'msg>> = Job.delay <| fun () ->
          let mA = Mailbox ()
          Job.start (body mA) >>-. mA

        let post (mA: Mailbox<'msg>) (m: 'msg) : Job<unit> = mA *<<+ m

        let postAndReply (mA: Mailbox<'msg>) (i2m: ReplyChannel<'r> -> 'msg) : Job<'r> = Job.delay <| fun () ->
          let i = ReplyChannel ()
          mA *<<+ i2m i >>=. i

        let reply (rI: ReplyChannel<'r>) (r: 'r) : Job<unit> = rI *<= r

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

    let inline reverse (str : string) =
        str.ToCharArray()
        |> Array.rev
        |> (fun x -> new string(x))

    let inline splitByCharMax (char : char) (max : int) (str : string) =
        str.Split([|char|],max)
module TypeInfo =
    open System

    let inline getType obj = obj.GetType()
    let inline getTypeName (``type`` : Type) = ``type``.Name

    let inline typeNameToCamelCase obj =
        obj
        |> getType
        |> getTypeName
        |> String.toCamelCase

module Disposable =
    open System
    open System.Threading

    type AnonymousDisposable(dispose : unit -> unit) =
        let mutable dipose = Action(dispose)
        member __.Dispose () =
            let disposer = Interlocked.Exchange<Action>(&dipose, null)
            if disposer |> isNull |> not then disposer.Invoke()

        member __.IsDisposed = dipose |> isNull
        interface IDisposable with
            member __.Dispose() = __.Dispose()

    type CTSCancelOnDispose (cts : CancellationTokenSource) =
        let mutable dipose = ""

        new () = new CTSCancelOnDispose(new CancellationTokenSource())

        member __.CancellationTokenSource = cts
        member __.Token = __.CancellationTokenSource.Token

        member __.Dispose () =
            let disposer = Interlocked.Exchange<string>(&dipose, null)
            if disposer |> isNull |> not then
                __.CancellationTokenSource.Cancel()
                __.CancellationTokenSource.Dispose()

        member __.IsDisposed = dipose |> isNull
        interface IDisposable with
            member __.Dispose() = __.Dispose()

    let inline create f = new AnonymousDisposable(f) :> IDisposable


module DbHelpers =
    open System
    open Npgsql
    open NpgsqlTypes
    open Hopac
    open System.Threading


    let inline builderToConnection (connStr : NpgsqlConnectionStringBuilder) =
        new NpgsqlConnection (connStr |> string)


    let inline ensureOpenCt ct (connection : NpgsqlConnection) = job {
        if not <| connection.FullState.HasFlag System.Data.ConnectionState.Open then
            do!  (connection.OpenAsync ct) |> Job.awaitUnitTask
    }

    let inline createOpenConnectionCt ct (connStr : NpgsqlConnectionStringBuilder) = job {
        let conn =
            connStr
            |> builderToConnection
        do! ensureOpenCt ct conn
        return conn
    }

    let inline executeReaderCt (ct: CancellationToken) (cmd : NpgsqlCommand) = job {
        let! reader = cmd.ExecuteReaderAsync ct
        return reader |> unbox<NpgsqlDataReader>
    }

    let inline executeScalarCt<'a> ct (cmd : NpgsqlCommand) =
        cmd.ExecuteScalarAsync ct
        |> Job.awaitTask
        |> Job.map (unbox<'a>)

    let inline executeNonQueryCt ct (cmd : NpgsqlCommand) =
        cmd.ExecuteNonQueryAsync ct |> Job.awaitUnitTask

    let inline executeNonQueryIgnoreCt ct (cmd : NpgsqlCommand) =
        executeNonQueryCt ct cmd
        |> Job.Ignore

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

    let inline readRow ct (reader: NpgsqlDataReader) =
        let readValueAsync fieldIndex =
          job {
              let fieldName = reader.GetName fieldIndex
              let! isNull = reader.IsDBNullAsync(fieldIndex,ct)
              if isNull then
                return fieldName, None
              else
                let! value = reader.GetFieldValueAsync(fieldIndex,ct)
                return fieldName, Some value
          }

        [0 .. reader.FieldCount - 1]
        |> List.map readValueAsync
        |> Job.seqCollect
        |> Job.map List.ofSeq

    let inline readTable ct (reader: NpgsqlDataReader) =
        let rec readRows rows = job {
            let! canRead = reader.ReadAsync(ct)
            if canRead then
              let! row = readRow ct reader
              return! readRows (row :: rows )
            else
              return rows
        }
        readRows []
        |> Job.map List.rev
    let inline readFirstRow ct  (reader: NpgsqlDataReader) =
        reader
        |> readTable ct
        |> Job.map Seq.tryHead

    let inline mapRow ct f =
        readTable ct
        >> Job.map (List.choose f)
