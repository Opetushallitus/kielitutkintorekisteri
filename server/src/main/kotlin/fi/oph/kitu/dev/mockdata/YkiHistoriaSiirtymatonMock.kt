package fi.oph.kitu.dev.mockdata

import fi.oph.kitu.yki.TutkinnonOsa
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import fi.oph.kitu.yki.historia.YkiHistoriaSiirtymatonEntity
import fi.oph.kitu.yki.historia.YkiHistoriaSiirtymattomyydenSyy
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import java.time.LocalDate
import kotlin.random.Random

/** Oppijanumerohaun kirjaamat syyt, samat merkkijonot kuin migraatioskriptissa oli. */
private val oidHaunSyyt =
    listOf(
        "virheellinen hetu (muoto)",
        "virheellinen hetu (tarkistusmerkki)",
        "virheellinen hetu (päivämäärä)",
        "hetu puuttuu",
        "ei löytynyt oppijanumerorekisteristä",
        "ei löytynyt oppijanumerorekisteristä (hetulista)",
        "ONR hylkäsi pyynnön: HTTP 400 {\"error\":\"BadRequest\"}",
    )

private val paikallisetSyyt =
    listOf(
        "ei yhtään osakoetta",
        "virheellinen arvosana tasolle PT: 7",
        "arvosanaMuuttui ei ole tarkistettujen osakokeiden osajoukko",
    )

/**
 * Lahteessa tarkistusarvioidut osakokeet ja muuttuneet arvosanat ovat bittimaskeja, joiden bitti
 * on 1 shl TutkinnonOsa-jarjestysluku. RS ja YL ovat jarjestyksessa vasta naiden jalkeen eivatka
 * voi olla tarkistusarvioituja.
 */
private val tarkistusarvioitavatOsakokeet =
    listOf(TutkinnonOsa.PU, TutkinnonOsa.KI, TutkinnonOsa.TY, TutkinnonOsa.PY)

private fun Collection<TutkinnonOsa>.toSolkiBittimaski(): Int = fold(0) { maski, osa -> maski or (1 shl osa.ordinal) }

/**
 * Yksi siirtymatta jaanyt historiarivi. Syy, syyluokka ja kenttien tyhjyys vastaavat
 * toisiaan samoin kuin oikeassa aineistossa: oppijanumeroton rivi on ilman oppijanumeroa,
 * rikkinaisella rivilla ei ole tunnistetta lainkaan ja "ei yhtaan osakoetta" tarkoittaa
 * ettei arvosanoja ole.
 */
fun generateRandomYkiHistoriaSiirtymatonEntity(
    minDate: LocalDate = LocalDate.of(2011, 1, 1),
    maxDate: LocalDate = LocalDate.of(2016, 12, 31),
    syyluokka: YkiHistoriaSiirtymattomyydenSyy = YkiHistoriaSiirtymattomyydenSyy.entries.random(),
): YkiHistoriaSiirtymatonEntity {
    val person = generateRandomPerson()
    val (tutkintopaiva, arviointipaiva, lastModified) = getRandomLocalDates(3, minDate, maxDate).sorted()
    val tutkintotaso = Tutkintotaso.entries.random()
    val solkiId = Random.nextInt(100000, 999999).toString()

    if (syyluokka == YkiHistoriaSiirtymattomyydenSyy.RIKKINAINEN_RIVI) {
        // Sarakemaara oli vaarin, joten paikkoihin ei voitu luottaa: vain rivi itse.
        val sarakkeita = (3..29).random()
        return YkiHistoriaSiirtymatonEntity(
            syy = "odotettiin 30 saraketta, saatiin $sarakkeita",
            syyluokka = syyluokka,
            rawRivi = List(sarakkeita) { "kenttä$it" }.joinToString(","),
            lahdetiedosto = LAHDETIEDOSTO,
        )
    }

    val syy =
        when (syyluokka) {
            YkiHistoriaSiirtymattomyydenSyy.EI_OPPIJANUMEROA -> {
                "ei oppijanumeroa"
            }

            YkiHistoriaSiirtymattomyydenSyy.PAIKALLINEN_VALIDOINTI -> {
                paikallisetSyyt.random()
            }

            YkiHistoriaSiirtymattomyydenSyy.API_HYLKASI -> {
                "HTTP 400: {\"tunniste\":\"virheellinenTieto\",\"viesti\":\"tutkintopaiva\"}"
            }

            YkiHistoriaSiirtymattomyydenSyy.LAST_MODIFIED_EI_JASENNY -> {
                "last_modified ei jäsenny, ei voi suodattaa"
            }

            else -> {
                "tuntematon virhe"
            }
        }

    val ilmanOppijanumeroa = syyluokka == YkiHistoriaSiirtymattomyydenSyy.EI_OPPIJANUMEROA
    val maxArvosana =
        when (tutkintotaso) {
            Tutkintotaso.PT -> 2
            Tutkintotaso.KT -> 4
            Tutkintotaso.YT -> 6
        }
    val arvosana = { if (syy == "ei yhtään osakoetta") null else (0..maxArvosana).random().toString() }
    val tarkistusarvioitu = Random.nextInt(100) < 5
    val tarkistusarvioidutOsakokeet =
        if (tarkistusarvioitu) {
            tarkistusarvioitavatOsakokeet.shuffled().take((1..tarkistusarvioitavatOsakokeet.size).random())
        } else {
            emptyList()
        }
    val arvosanaMuuttuneet = tarkistusarvioidutOsakokeet.filter { Random.nextBoolean() }

    return YkiHistoriaSiirtymatonEntity(
        suorittajanOid = if (ilmanOppijanumeroa) null else person.oppijanumero.toString(),
        // Katkaistu hetu (DDMMYY-) on aineiston yleisin syy sille ettei oppijanumeroa loydy.
        hetu = if (ilmanOppijanumeroa && Random.nextBoolean()) person.hetu.take(7) else person.hetu,
        sukupuoli = person.sukupuoli.name,
        sukunimi = person.sukunimi,
        etunimet = person.etunimet,
        kansalaisuus = person.kansalaisuus,
        katuosoite = person.katuosoite,
        postinumero = person.postinumero,
        postitoimipaikka = person.postitoimipaikka,
        email = person.email,
        solkiId = solkiId,
        lastModified =
            if (syyluokka == YkiHistoriaSiirtymattomyydenSyy.LAST_MODIFIED_EI_JASENNY) {
                "0000-00-00 00:00:00"
            } else {
                "${lastModified}T00:00:00Z"
            },
        tutkintopaiva = tutkintopaiva.toString(),
        tutkintokieli =
            Tutkintokieli.entries
                .minus(Tutkintokieli.legacyEntries)
                .random()
                .solkiCode,
        tutkintotaso = tutkintotaso.name,
        jarjestajanOid = generateRandomOrganizationOid().toString(),
        jarjestajanNimi = "${person.postitoimipaikka}n yliopisto",
        arviointipaiva = arviointipaiva.toString(),
        asTy = arvosana(),
        asKi = arvosana(),
        asRs = arvosana(),
        asPy = arvosana(),
        asPu = arvosana(),
        asYl = arvosana(),
        tarkSaapumisPvm = if (tarkistusarvioitu) arviointipaiva.plusDays(7).toString() else null,
        tarkAsiatunnus = if (tarkistusarvioitu) "OPH-${(1..9999).random()}-${tutkintopaiva.year}" else null,
        tarkOsakokeet = if (tarkistusarvioitu) tarkistusarvioidutOsakokeet.toSolkiBittimaski().toString() else null,
        arvosanaMuuttui = if (tarkistusarvioitu) arvosanaMuuttuneet.toSolkiBittimaski().toString() else null,
        perustelu = if (tarkistusarvioitu) listOf("Erinomainen", "Hyvä", "Tyydyttävä").random() else null,
        tarkKasittelyPvm = if (tarkistusarvioitu) arviointipaiva.plusDays(30).toString() else null,
        syy = syy,
        syyluokka = syyluokka,
        // Tyhja = hakua ei yritetty talle riville, mika on eri asia kuin haku joka hylkasi.
        oidHaunSyy = if (ilmanOppijanumeroa && Random.nextInt(4) > 0) oidHaunSyyt.random() else null,
        lahdetiedosto = LAHDETIEDOSTO,
    )
}

private const val LAHDETIEDOSTO = "yki-historia-mock.csv"

/**
 * Tuotantokoodi ei kirjoita tahan tauluun - rivit tulevat CloudShellista ajettavalla
 * latausskriptilla RDS Data API:n kautta - joten kirjoitus elaa vain mockdatan puolella.
 */
fun NamedParameterJdbcTemplate.insertSiirtymattomatMockRivit(
    rivit: List<YkiHistoriaSiirtymatonEntity>,
): List<YkiHistoriaSiirtymatonEntity> {
    batchUpdate(
        """
        INSERT INTO yki_historia_siirtymaton (
            suorittajan_oid, hetu, sukupuoli, sukunimi, etunimet, kansalaisuus, katuosoite,
            postinumero, postitoimipaikka, email, solki_id, last_modified, tutkintopaiva,
            tutkintokieli, tutkintotaso, jarjestajan_oid, jarjestajan_nimi, arviointipaiva,
            as_ty, as_ki, as_rs, as_py, as_pu, as_yl, tark_saapumis_pvm, tark_asiatunnus,
            tark_osakokeet, arvosana_muuttui, perustelu, tark_kasittely_pvm,
            syy, syyluokka, oid_haun_syy, raw_rivi, lahdetiedosto
        )
        VALUES (
            :suorittajan_oid, :hetu, :sukupuoli, :sukunimi, :etunimet, :kansalaisuus, :katuosoite,
            :postinumero, :postitoimipaikka, :email, :solki_id, :last_modified, :tutkintopaiva,
            :tutkintokieli, :tutkintotaso, :jarjestajan_oid, :jarjestajan_nimi, :arviointipaiva,
            :as_ty, :as_ki, :as_rs, :as_py, :as_pu, :as_yl, :tark_saapumis_pvm, :tark_asiatunnus,
            :tark_osakokeet, :arvosana_muuttui, :perustelu, :tark_kasittely_pvm,
            :syy, CAST(:syyluokka AS yki_historia_siirtymattomyyden_syy), :oid_haun_syy,
            :raw_rivi, :lahdetiedosto
        )
        ON CONFLICT (solki_id) DO NOTHING
        """.trimIndent(),
        rivit.map { it.toSqlParams() }.toTypedArray(),
    )
    return rivit
}

private fun YkiHistoriaSiirtymatonEntity.toSqlParams(): MapSqlParameterSource =
    MapSqlParameterSource()
        .addValue("suorittajan_oid", suorittajanOid)
        .addValue("hetu", hetu)
        .addValue("sukupuoli", sukupuoli)
        .addValue("sukunimi", sukunimi)
        .addValue("etunimet", etunimet)
        .addValue("kansalaisuus", kansalaisuus)
        .addValue("katuosoite", katuosoite)
        .addValue("postinumero", postinumero)
        .addValue("postitoimipaikka", postitoimipaikka)
        .addValue("email", email)
        .addValue("solki_id", solkiId)
        .addValue("last_modified", lastModified)
        .addValue("tutkintopaiva", tutkintopaiva)
        .addValue("tutkintokieli", tutkintokieli)
        .addValue("tutkintotaso", tutkintotaso)
        .addValue("jarjestajan_oid", jarjestajanOid)
        .addValue("jarjestajan_nimi", jarjestajanNimi)
        .addValue("arviointipaiva", arviointipaiva)
        .addValue("as_ty", asTy)
        .addValue("as_ki", asKi)
        .addValue("as_rs", asRs)
        .addValue("as_py", asPy)
        .addValue("as_pu", asPu)
        .addValue("as_yl", asYl)
        .addValue("tark_saapumis_pvm", tarkSaapumisPvm)
        .addValue("tark_asiatunnus", tarkAsiatunnus)
        .addValue("tark_osakokeet", tarkOsakokeet)
        .addValue("arvosana_muuttui", arvosanaMuuttui)
        .addValue("perustelu", perustelu)
        .addValue("tark_kasittely_pvm", tarkKasittelyPvm)
        .addValue("syy", syy)
        .addValue("syyluokka", syyluokka.name)
        .addValue("oid_haun_syy", oidHaunSyy)
        .addValue("raw_rivi", rawRivi)
        .addValue("lahdetiedosto", lahdetiedosto)
