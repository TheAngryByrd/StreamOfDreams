namespace StreamOfDreams.Tests

open System
open Npgsql
open System.Reflection
open SimpleMigrations
open SimpleMigrations.DatabaseProvider

[<AutoOpen>]
module DatabaseTestHelpers =
    let execNonQuery connStr commandStr =
        use conn = new NpgsqlConnection(connStr)
        use cmd = new NpgsqlCommand(commandStr,conn)
        conn.Open()
        cmd.ExecuteNonQuery()

    let createDatabase connStr databaseName =
        databaseName
        |> sprintf "CREATE database \"%s\" ENCODING = 'UTF8'"
        |> execNonQuery connStr
        |> ignore

    let dropDatabase connStr databaseName =
        //kill out all connections
        databaseName
        |> sprintf "select pg_terminate_backend(pid) from pg_stat_activity where datname='%s';"
        |> execNonQuery connStr
        |> ignore

        databaseName
        |> sprintf "DROP database \"%s\""
        |> execNonQuery connStr
        |> ignore

    let createConnString host user pass database =
        sprintf "Host=%s;Username=%s;Password=%s;Database=%s" host user pass database
        |> NpgsqlConnectionStringBuilder

    let duplicateConn (conn : NpgsqlConnectionStringBuilder) =
        conn |> string |> NpgsqlConnectionStringBuilder

    let duplicateAndChange f conn =
        let duplicate = duplicateConn conn
        duplicate |> f
        duplicate


    type DisposableDatabase (superConn : NpgsqlConnectionStringBuilder, databaseName : string) =
        static member Create(connStr) =
            let databaseName = System.Guid.NewGuid().ToString("n")
            createDatabase (connStr |> string) databaseName

            new DisposableDatabase(connStr,databaseName)
        member __.SuperConn = superConn
        member x.Conn =
            x.SuperConn
            |> duplicateAndChange (fun conn ->
                conn.Database <- x.DatabaseName
            )

        member x.DatabaseName = databaseName
        interface IDisposable with
            member x.Dispose() =
                dropDatabase (superConn |> string) databaseName

    let getEnv str =
        System.Environment.GetEnvironmentVariable str

    let host () = "localhost"// getEnv "POSTGRES_HOST"
    let user () = "jimmybyrd"//getEnv "POSTGRES_USER"
    let pass () = "postgres" //getEnv "POSTGRES_PASS"
    let db () = "postgres" //getEnv "POSTGRES_DB"
    let superUserConnStr () = createConnString (host ()) (user ()) (pass()) (db())

    let getNewDatabase () = superUserConnStr () |>  DisposableDatabase.Create


    let doMigration (migrationsAssembly : Assembly) connStr =
        use connection = new NpgsqlConnection(connStr)

        let databaseProvider = new PostgresqlDatabaseProvider(connection)
        let migrator = new SimpleMigrator(migrationsAssembly, databaseProvider)
        migrator.Load()
        migrator.MigrateToLatest()

    let getMigratedDatabase () =
        let db = getNewDatabase ()
        db.Conn
        |> string
        |> doMigration typedefof<StreamOfDreams.DbTypes.Event>.Assembly
        db
