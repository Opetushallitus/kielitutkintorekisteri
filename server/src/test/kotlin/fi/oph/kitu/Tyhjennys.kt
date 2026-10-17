package fi.oph.kitu

import org.springframework.jdbc.core.JdbcTemplate

fun JdbcTemplate.tyhjennaYkiSuoritukset() {
    execute("TRUNCATE TABLE yki_suoritus_lisatieto")
    execute("TRUNCATE TABLE yki_suoritus CASCADE")
}

fun JdbcTemplate.tyhjennaArvioijahakemukset() {
    update("DELETE FROM yki_arvioijahakemus")
}
