DO $$ 
DECLARE 
    table_name TEXT;
    sequence_name TEXT;
BEGIN
    -- Loop through all tables in the specified schema (replace 'omar' with your schema name)
    FOR table_name IN 
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = 'omar'  -- specify your schema here
    LOOP
        -- Try to get the sequence associated with the 'id' column in each table
        BEGIN
            -- Get the sequence name for the 'id' column of the current table
            SELECT pg_get_serial_sequence('omar.' || table_name, 'id') INTO sequence_name;
            
            -- If the sequence exists, reset it
            IF sequence_name IS NOT NULL THEN
                -- Reset the sequence to the maximum 'id' value in the table
                EXECUTE format('SELECT setval(%L, (SELECT MAX(id) FROM omar.%I))', sequence_name, table_name);
                RAISE NOTICE 'Sequence for table % reset successfully.', table_name;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            -- If an error occurs (e.g., no 'id' column), skip to the next table
            RAISE NOTICE 'No auto-increment column found for table % or other error.', table_name;
        END;
    END LOOP;
    
    -- Final notice
    RAISE NOTICE 'All sequences reset successfully.';
END $$;



DO $$ 
DECLARE 
    table_name TEXT;
    sequence_name TEXT;
    column_name TEXT;
BEGIN
    -- Loop through all tables in the specified schema (replace 'omar' with your schema name)
    FOR table_name IN 
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = 'omar'  -- specify your schema here
    LOOP
        -- Try to get the sequence associated with the 'id' column in each table
        BEGIN
            -- Get the sequence name for the 'id' column of the current table
            SELECT pg_get_serial_sequence('omar.' || table_name, 'id') INTO sequence_name;
            
            -- If the sequence exists, reset it for the 'id' column
            IF sequence_name IS NOT NULL THEN
                EXECUTE format('SELECT setval(%L, (SELECT MAX(id) FROM omar.%I))', sequence_name, table_name);
                RAISE NOTICE 'Sequence for "id" column in table % reset successfully.', table_name;
            END IF;

            -- Check for any 'history_id' column and reset its sequence if it exists
            BEGIN
                -- Get the sequence for the 'history_id' column (if it exists)
                SELECT pg_get_serial_sequence('omar.' || table_name, 'history_id') INTO sequence_name;

                -- Reset sequence for 'history_id' column
                IF sequence_name IS NOT NULL THEN
                    EXECUTE format('SELECT setval(%L, (SELECT MAX(history_id) FROM omar.%I))', sequence_name, table_name);
                    RAISE NOTICE 'Sequence for "history_id" column in table % reset successfully.', table_name;
                END IF;
            EXCEPTION WHEN OTHERS THEN
                -- If the 'history_id' column doesn't exist, just skip it
                RAISE NOTICE 'No "history_id" column or other error for table %.', table_name;
            END;
        EXCEPTION WHEN OTHERS THEN
            -- If an error occurs, skip to the next table
            RAISE NOTICE 'Error processing table %: %', table_name, SQLERRM;
        END;
    END LOOP;
    
    -- Final notice
    RAISE NOTICE 'All sequences reset successfully.';
END $$;



DO $$ 
DECLARE 
    table_name TEXT;
    sequence_name TEXT;
BEGIN
    -- Loop through all tables in the specified schema
    FOR table_name IN 
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = 'fde-local'
    LOOP
        -- Try to get the sequence associated with the 'id' column
        BEGIN
            SELECT pg_get_serial_sequence('fde-local.' || table_name, 'id') INTO sequence_name;
            IF sequence_name IS NOT NULL THEN
                EXECUTE format(
                    'SELECT setval(%L, COALESCE((SELECT MAX(id) FROM "fde-local".%I), 0) + 1, false)',
                    sequence_name, table_name
                );
                RAISE NOTICE 'Sequence for "id" column in table % reset successfully.', table_name;
            END IF;
 
            -- Now handle 'history_id' if it exists
            BEGIN
                SELECT pg_get_serial_sequence('fde-local.' || table_name, 'history_id') INTO sequence_name;
 
                IF sequence_name IS NOT NULL THEN
                    EXECUTE format(
                        'SELECT setval(%L, COALESCE((SELECT MAX(history_id) FROM "fde-local".%I), 0) + 1, false)',
                        sequence_name, table_name
                    );
                    RAISE NOTICE 'Sequence for "history_id" column in table % reset successfully.', table_name;
                END IF;
            EXCEPTION WHEN OTHERS THEN
                RAISE NOTICE 'No "history_id" column or other error for table %.', table_name;
            END;
 
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Error processing table %: %', table_name, SQLERRM;
        END;
    END LOOP;
    RAISE NOTICE 'All sequences reset successfully.';
END $$;