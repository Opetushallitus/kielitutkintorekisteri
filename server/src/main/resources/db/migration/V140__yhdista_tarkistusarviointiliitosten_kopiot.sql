DROP TABLE IF EXISTS yhdistetyt_liitokset;

CREATE TEMPORARY TABLE yhdistetyt_liitokset AS
SELECT osakoe_id,
       tarkistusarviointi_id,
       bool_or(arvosana_muuttui) AS arvosana_muuttui
FROM yki_osakoe_tarkistusarviointi
GROUP BY osakoe_id, tarkistusarviointi_id
HAVING count(*) > 1;

DELETE
FROM yki_osakoe_tarkistusarviointi liitos
    USING yhdistetyt_liitokset yhdistetty
WHERE liitos.osakoe_id = yhdistetty.osakoe_id
  AND liitos.tarkistusarviointi_id = yhdistetty.tarkistusarviointi_id;

INSERT INTO yki_osakoe_tarkistusarviointi (osakoe_id, tarkistusarviointi_id, arvosana_muuttui)
SELECT osakoe_id, tarkistusarviointi_id, arvosana_muuttui
FROM yhdistetyt_liitokset;

DROP TABLE IF EXISTS yhdistetyt_liitokset;

ALTER TABLE yki_osakoe_tarkistusarviointi
    DROP CONSTRAINT IF EXISTS yki_osakoe_tarkistusarviointi_unique;

ALTER TABLE yki_osakoe_tarkistusarviointi
    ADD CONSTRAINT yki_osakoe_tarkistusarviointi_unique UNIQUE (osakoe_id, tarkistusarviointi_id);

DROP INDEX IF EXISTS yki_osakoe_tarkistusarviointi_osakoe_id_idx;
