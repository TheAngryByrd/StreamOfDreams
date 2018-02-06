namespace StreamOfDreams

module Migrations =
    open SimpleMigrations

    [<Migration(20180204123000L, "Create uuid extension")>]
    type ``Create uuid extension`` () =
        inherit Migration()
            override __.Up () =
                base.Execute
                    """
                    CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
                    """
            override __.Down () =
                // Not gonna delete it in case other things are using it.
                ()


    [<Migration(20180204124000L, "Create streams table")>]
    type ``Create streams table`` () =
        inherit Migration()
            override __.Up () =
                base.Execute
                    """
                    CREATE TABLE streams
                    (
                        stream_id bigserial PRIMARY KEY NOT NULL,
                        stream_name text NOT NULL,
                        stream_version bigint default 0 NOT NULL,
                        created_at timestamp without time zone default (now() at time zone 'utc') NOT NULL
                    );
                    """
            override __.Down () =
                base.Execute
                    """
                    DROP TABLE streams
                    """

    [<Migration(20180204124001L, "Create stream_name Index")>]
    type ``Create stream_name Index`` () =
        inherit Migration()
            override __.Up () =
                base.Execute
                    """
                    CREATE UNIQUE INDEX ix_streams_stream_name ON streams (stream_name);
                    """
            override __.Down () =
                base.Execute
                    """
                    DROP INDEX ix_streams_stream_name;
                    """

    [<Migration(20180204124002L, "Seed all stream")>]
    type ``Seed all stream`` () =
        inherit Migration()
            override __.Up () =
                base.Execute
                    """
                    INSERT INTO streams (stream_id, stream_name, stream_version) VALUES (0, '$all', 0);
                    """
            override __.Down () =
                base.Execute
                    """
                    DELETE FROM streams where stream_id = 0;
                    """

    [<Migration(20180204124003L, "Create events table")>]
    type ``Create events table`` () =
        inherit Migration()
            override __.Up () =
                base.Execute
                    """CREATE TABLE events
                    (
                        event_id uuid PRIMARY KEY NOT NULL DEFAULT uuid_generate_v4(),
                        event_type text NOT NULL,
                        causation_id uuid NULL,
                        correlation_id uuid NULL,
                        data jsonb NOT NULL,
                        metadata jsonb NULL,
                        created_at timestamp without time zone default (now() at time zone 'utc') NOT NULL
                    );"""
            override __.Down () =
                base.Execute
                    """
                    DROP TABLE events
                    """


    [<Migration(20180204124005L, "Prevent events updates")>]
    type ``Prevent events updates`` () =
        inherit Migration()
            override __.Up () =
                base.Execute
                    """
                    CREATE RULE no_update_events AS ON UPDATE TO events DO INSTEAD NOTHING;
                    """
            override __.Down () =
                base.Execute
                    """
                    DROP RULE no_update_events ON events
                    """


    [<Migration(20180204124006L, "Prevent events deletes")>]
    type ``Prevent events delete`` () =
        inherit Migration()
            override __.Up () =
                base.Execute
                    """
                    CREATE RULE no_delete_events AS ON DELETE TO events DO INSTEAD NOTHING;
                    """
            override __.Down () =
                base.Execute
                    """
                    DROP RULE no_delete_events ON events
                    """


    [<Migration(20180204124007L, "Create Stream Events Table")>]
    type ``Create Stream Events Table`` () =
        inherit Migration()
            override __.Up () =
                base.Execute
                    """
                    CREATE TABLE stream_events
                    (
                      event_id uuid NOT NULL REFERENCES events (event_id),
                      stream_id bigint NOT NULL REFERENCES streams (stream_id),
                      stream_version bigint NOT NULL,
                      original_stream_id bigint REFERENCES streams (stream_id),
                      original_stream_version bigint,
                      PRIMARY KEY(event_id, stream_id)
                    );
                    """
            override __.Down () =
                base.Execute
                    """
                    DROP TABLE stream_events
                    """


    [<Migration(20180204124008L, "Create Stream Events index")>]
    type ``Create Stream Events index`` () =
        inherit Migration()
            override __.Up () =
                base.Execute
                    """
                    CREATE UNIQUE INDEX ix_stream_events ON stream_events (stream_id, stream_version);
                    """
            override __.Down () =
                base.Execute
                    """
                    DROP INDEX ix_stream_events;
                    """


    [<Migration(20180204124009L, "Create Notify Events Function")>]
    type ``Create Notify Events Function`` () =
        inherit Migration()
            override __.Up () =
                base.Execute
                    """
                    CREATE OR REPLACE FUNCTION notify_events()
                      RETURNS trigger AS $$
                    DECLARE
                      payload text;
                    BEGIN
                        -- Payload text contains:
                        --  * `stream_name`
                        --  * `stream_id`
                        --  * first `stream_version`
                        --  * last `stream_version`
                        -- Each separated by a comma (e.g. 'stream-12345,1,1,5')
                        payload := NEW.stream_name || ',' || NEW.stream_id || ',' || (OLD.stream_version + 1) || ',' || NEW.stream_version;
                        -- Notify events to listeners
                        PERFORM pg_notify('events', payload);
                        RETURN NULL;
                    END;
                    $$ LANGUAGE plpgsql;
                    """

            override __.Down () =
                base.Execute
                    """
                    DROP FUNCTION notify_events();
                    """

    [<Migration(20180204124010L, "Create Event Notification Trigger")>]
    type ``Create Event Notification Trigger`` () =
        inherit Migration()
            override __.Up () =
                base.Execute
                    """
                    CREATE TRIGGER event_notification
                    AFTER UPDATE ON streams
                    FOR EACH ROW EXECUTE PROCEDURE notify_events();
                    """

            override __.Down () =
                base.Execute
                    """
                    DROP TRIGGER event_notification ON streams;
                    """

    [<Migration(20180204124011L, "Create Subscriptions Table")>]
    type ``Create Subscriptions Table`` () =
        inherit Migration()
            override __.Up () =
                base.Execute
                    """
                    CREATE TABLE subscriptions
                    (
                        subscription_id bigserial PRIMARY KEY NOT NULL,
                        stream_uuid text NOT NULL,
                        subscription_name text NOT NULL,
                        last_seen bigint NULL,
                        created_at timestamp without time zone default (now() at time zone 'utc') NOT NULL
                    );
                    """

            override __.Down () =
                base.Execute
                    """
                    DROP TABLE subscriptions;
                    """

    [<Migration(20180204124012L, "Create Subscriptions index")>]
    type ``Create Subscriptions index`` () =
        inherit Migration()
            override __.Up () =
                base.Execute
                    """
                    CREATE UNIQUE INDEX ix_subscriptions_stream_uuid_subscription_name ON subscriptions (stream_uuid, subscription_name);
                    """
            override __.Down () =
                base.Execute
                    """
                    DROP INDEX ix_subscriptions_stream_uuid_subscription_name;
                    """
    [<Migration(20180204124013L, "Create Snapshots Table")>]
    type ``Create Snapshots Table`` () =
        inherit Migration()
            override __.Up () =
                base.Execute
                    """
                    CREATE TABLE snapshots
                    (
                        source_uuid text PRIMARY KEY NOT NULL,
                        source_version bigint NOT NULL,
                        source_type text NOT NULL,
                        data jsonb NOT NULL,
                        metadata jsonb NULL,
                        created_at timestamp without time zone default (now() at time zone 'utc') NOT NULL
                    );
                    """

            override __.Down () =
                base.Execute
                    """
                    DROP TABLE snapshots;
                    """
