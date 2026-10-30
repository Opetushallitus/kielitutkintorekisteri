CREATE INDEX "vkt_osakoe_suoritus_id_idx"
    ON "vkt_osakoe" ("suoritus_id");

CREATE INDEX "vkt_tutkinto_suoritus_id_idx"
    ON "vkt_tutkinto" ("suoritus_id");

CREATE INDEX "vkt_suoritus_ilmoittautumisen_id_idx"
    ON "vkt_suoritus" ("ilmoittautumisen_id", "created_at" DESC);

CREATE INDEX "vkt_suoritus_tutkintoryhma_idx"
    ON "vkt_suoritus" ("suorittajan_oid", "tutkintokieli", "taitotaso");

CREATE INDEX "yki_osakoe_tarkistusarviointi_tarkistusarviointi_id_idx"
    ON "yki_osakoe_tarkistusarviointi" ("tarkistusarviointi_id");
