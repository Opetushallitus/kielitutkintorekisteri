package fi.oph.kitu.dev.mockdata

import fi.oph.kitu.yki.suoritukset.YkiSuoritusEntity
import fi.oph.kitu.yki.suoritukset.error.YkiSuoritusErrorEntity
import java.time.Instant
import kotlin.reflect.full.memberProperties

fun generateRandomYkiSuoritusErrorEntity(): YkiSuoritusErrorEntity {
    val lastModified = getRandomInstant(Instant.parse("2004-01-01T00:00:00Z"))
    val virheenLuontiaika = getRandomInstant(lastModified)
    val virheellinenKentta = YkiSuoritusEntity::class.memberProperties.random().name

    val suoritusEntity = generateRandomYkiSuoritusEntity()

    return YkiSuoritusErrorEntity(
        id = null,
        suorittajanOid = suoritusEntity.suorittajanOID?.toString(),
        hetu = suoritusEntity.hetu,
        nimi = "${suoritusEntity.sukunimi} ${suoritusEntity.etunimet}",
        lastModified = lastModified,
        virheellinenKentta = virheellinenKentta,
        virheellinenArvo = "virheellinen_arvo",
        virheellinenRivi = suoritusEntity.toCsvString(),
        virheenRivinumero = (0..1000).random(),
        virheenLuontiaika = virheenLuontiaika,
    )
}

private fun YkiSuoritusEntity.toCsvString(): String =
    listOf(
        suorittajanOID,
        hetu,
        sukupuoli,
        sukunimi,
        etunimet,
        kansalaisuus,
        katuosoite,
        postinumero,
        postitoimipaikka,
        email,
        solkiId,
        lastModified,
        tutkintopaiva,
        tutkintokieli,
        tutkintotaso,
        jarjestajanTunnusOid,
        jarjestajanNimi,
        arviointipaiva,
        tekstinYmmartaminen,
        kirjoittaminen,
        rakenteetJaSanasto,
        puheenYmmartaminen,
        puhuminen,
        yleisarvosana,
        tarkistusarvioinninSaapumisPvm,
        tarkistusarvioinninAsiatunnus,
        tarkistusarvioidutOsakokeet?.joinToString(" "),
        arvosanaMuuttui?.joinToString(" "),
        perustelu,
        tarkistusarvioinninKasittelyPvm,
    ).joinToString(",") { it?.toString() ?: "" }
