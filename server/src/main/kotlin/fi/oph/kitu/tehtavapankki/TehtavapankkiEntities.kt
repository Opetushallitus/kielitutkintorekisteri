package fi.oph.kitu.tehtavapankki

import fi.oph.kitu.jdbc.getOffsetDateTimeOrNull
import fi.oph.kitu.util.defaultObjectMapper
import org.springframework.jdbc.core.RowMapper
import tools.jackson.databind.JsonNode
import java.time.OffsetDateTime

/**
 * Yleinen kysymyspankin paketti — yksi rivi yhtä lähdejärjestelmän + lähde-id:n + version
 * yhdistelmää kohti. Jokainen uusi sisältö (uusi versio_hash) tallennetaan omana rivinään
 * niin että historia säilyy ja import-deduplikaatio palautuu yksittäiseen UNIQUE-tarkistukseen.
 *
 * `versio_hash` lasketaan _alkuperäisestä_ lähdedatasta (esim. raaka XML), ei parsitusta
 * mallista, jotta sama lähdetiedosto antaa aina saman hashin parserin muutoksista
 * riippumatta.
 */
data class TehtavapakettiEntity(
    val id: Int? = null,
    val lahdejarjestelma: String,
    val lahdeId: String,
    val nimi: String,
    val versioHash: String,
    val s3Avain: String? = null,
    val metadata: JsonNode = defaultObjectMapper.createObjectNode(),
    val luotu: OffsetDateTime? = null,
    val lahdeFilegenerated: OffsetDateTime? = null,
    val lahdePublished: OffsetDateTime? = null,
    val lahdeVersion: String? = null,
    val lahdeLanguage: String? = null,
) {
    companion object {
        val fromRow: RowMapper<TehtavapakettiEntity> =
            RowMapper { rs, _ ->
                TehtavapakettiEntity(
                    id = rs.getInt("id"),
                    lahdejarjestelma = rs.getString("lahdejarjestelma"),
                    lahdeId = rs.getString("lahde_id"),
                    nimi = rs.getString("nimi"),
                    versioHash = rs.getString("versio_hash"),
                    s3Avain = rs.getString("s3_avain"),
                    metadata = defaultObjectMapper.readTree(rs.getString("metadata")),
                    luotu = rs.getOffsetDateTimeOrNull("luotu"),
                    lahdeFilegenerated = rs.getOffsetDateTimeOrNull("lahde_filegenerated"),
                    lahdePublished = rs.getOffsetDateTimeOrNull("lahde_published"),
                    lahdeVersion = rs.getString("lahde_version"),
                    lahdeLanguage = rs.getString("lahde_language"),
                )
            }
    }
}

/**
 * Tehtäväryhmä — Moodlessa `<question type="category">` luo kullekin paketille
 * loogisia jakoja, joihin tehtävät kuuluvat. `nimi` vastaa lähdejärjestelmän
 * antamaa polkua/otsikkoa, `jarjestys` säilyttää lähdeasiakirjan järjestyksen.
 */
data class TehtavaryhmaEntity(
    val id: Int? = null,
    val pakettiId: Int,
    val nimi: String,
    val jarjestys: Int,
    val metadata: JsonNode = defaultObjectMapper.createObjectNode(),
) {
    companion object {
        val fromRow: RowMapper<TehtavaryhmaEntity> =
            RowMapper { rs, _ ->
                TehtavaryhmaEntity(
                    id = rs.getInt("id"),
                    pakettiId = rs.getInt("paketti_id"),
                    nimi = rs.getString("nimi"),
                    jarjestys = rs.getInt("jarjestys"),
                    metadata = defaultObjectMapper.readTree(rs.getString("metadata")),
                )
            }
    }
}

/**
 * Yksittäinen kysymys / tehtävä paketissa. Tyyppikohtaiset kentät (esim. Moodlen
 * `defaultgrade`/`penalty`, `single`/`shuffleanswers`, cloudpoodllin `language`/
 * `audioskin`) tallennetaan `metadata`-jsonbiin — tässä taulussa pidetään vain
 * lähteiden välillä yhteiset, kyselyihin tarvittavat sarakkeet.
 *
 * `ryhmaId` viittaa tehtäväryhmään, jonka ingest-kerros määrittää
 * lähdejärjestelmän kategoriaelementtien perusteella.
 */
data class TehtavaEntity(
    val id: Int? = null,
    val pakettiId: Int,
    val ryhmaId: Int,
    val tyyppi: String,
    val lahdeId: String? = null,
    val nimi: String? = null,
    val teksti: String? = null,
    val tekstinFormaatti: String? = null,
    val jarjestys: Int,
    val metadata: JsonNode = defaultObjectMapper.createObjectNode(),
    val luotu: OffsetDateTime? = null,
) {
    companion object {
        val fromRow: RowMapper<TehtavaEntity> =
            RowMapper { rs, _ ->
                TehtavaEntity(
                    id = rs.getInt("id"),
                    pakettiId = rs.getInt("paketti_id"),
                    ryhmaId = rs.getInt("ryhma_id"),
                    tyyppi = rs.getString("tyyppi"),
                    lahdeId = rs.getString("lahde_id"),
                    nimi = rs.getString("nimi"),
                    teksti = rs.getString("teksti"),
                    tekstinFormaatti = rs.getString("tekstin_formaatti"),
                    jarjestys = rs.getInt("jarjestys"),
                    metadata = defaultObjectMapper.readTree(rs.getString("metadata")),
                    luotu = rs.getOffsetDateTimeOrNull("luotu"),
                )
            }
    }
}

/**
 * Tehtävän liitetiedosto S3:ssa (esim. Moodlen `<file>`-blobeista puretut
 * mp3:t ja png:t). `s3_avain` on koko polku bucketissa, esim.
 * `42-Suomi/2026-01-01 assets/audio.mp3`.
 */
data class TehtavaTiedostoEntity(
    val id: Int? = null,
    val tehtavaId: Int,
    val tiedostonimi: String,
    val s3Avain: String,
) {
    companion object {
        val fromRow: RowMapper<TehtavaTiedostoEntity> =
            RowMapper { rs, _ ->
                TehtavaTiedostoEntity(
                    id = rs.getInt("id"),
                    tehtavaId = rs.getInt("tehtava_id"),
                    tiedostonimi = rs.getString("tiedostonimi"),
                    s3Avain = rs.getString("s3_avain"),
                )
            }
    }
}

/**
 * Vastausvaihtoehto / hyväksytty vastaus tehtävälle. Lähdejärjestelmäkohtaiset
 * arvostelu- ja palautekentät (esim. Moodlen `fraction`/`feedback`) tallennetaan
 * `metadata`-jsonbiin yhteneväisesti `tehtava`-taulun kanssa.
 */
data class TehtavaVastausEntity(
    val id: Int? = null,
    val tehtavaId: Int,
    val jarjestys: Int,
    val teksti: String? = null,
    val tekstinFormaatti: String? = null,
    val metadata: JsonNode = defaultObjectMapper.createObjectNode(),
) {
    companion object {
        val fromRow: RowMapper<TehtavaVastausEntity> =
            RowMapper { rs, _ ->
                TehtavaVastausEntity(
                    id = rs.getInt("id"),
                    tehtavaId = rs.getInt("tehtava_id"),
                    jarjestys = rs.getInt("jarjestys"),
                    teksti = rs.getString("teksti"),
                    tekstinFormaatti = rs.getString("tekstin_formaatti"),
                    metadata = defaultObjectMapper.readTree(rs.getString("metadata")),
                )
            }
    }
}
