ALTER TABLE koto_suoritus_error
    ADD COLUMN kurssi_id    INTEGER,
    ADD COLUMN kurssi       TEXT,
    ADD COLUMN suoritusaika TIMESTAMP WITH TIME ZONE;
