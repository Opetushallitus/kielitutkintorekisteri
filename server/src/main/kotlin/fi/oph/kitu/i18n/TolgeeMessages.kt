package fi.oph.kitu.i18n

import fi.oph.kitu.util.defaultObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.core.io.ClassPathResource
import tools.jackson.module.kotlin.readValue

internal const val KAANNOSHAKEMISTO = "lokalisointi"

/**
 * Käännösvarasto kahdessa kerroksessa.
 *
 * *Lähdetekstit* luetaan luokkapolulta (`lokalisointi/{fi,sv,en}.json`). Ne ovat repon versioitu
 * totuus ja käytettävissä jokaisessa profiilissa ilman verkkoa. *Elävä* kerros tulee
 * lokalisointipalvelusta [LokalisointiLoader]in kautta.
 *
 * Yhdistely on **kielikohtainen**: elävä arvo voittaa kielen kerrallaan ja lähdeteksti täyttää
 * puuttuvat. Avainkohtainen korvaus pyyhkisi lähdetekstien ruotsin niiltä avaimilta, joista proxy
 * tarjoaa vain suomen. Tulos esilasketaan kirjoitushetkellä, jotta [get] pysyy yhtenä map-hakuna
 * renderöinnin kuumalla polulla.
 *
 * Lähdetekstit ladataan luokan alustuksessa eikä `ApplicationRunner`illa, koska ne ovat
 * renderöinnin edellytys ja runnerit ajetaan vasta kun palvelin ottaa jo pyyntöjä vastaan.
 */
object TolgeeMessages {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Volatile
    private var store: Map<String, LocalizedString> = emptyMap()

    @Volatile
    private var sourceTexts: Map<String, LocalizedString> = readSourceTexts()

    @Volatile
    private var merged: Map<String, LocalizedString> = sourceTexts

    fun get(key: String): LocalizedString? = merged[key]

    /** Koodin lähdeteksti, jonka Tolgee-synkronointi työntää uuden avaimen mukana. */
    fun sourceText(key: String): String? = sourceTexts[key]?.fi

    fun sourceKeys(): Set<String> = sourceTexts.keys

    @Synchronized
    fun set(messages: Map<String, LocalizedString>) {
        store = messages
        merge()
    }

    fun clear() = set(emptyMap())

    /** Vain testejä varten; tuotannossa lähdetekstit tulevat luokkapolulta. */
    @Synchronized
    fun setSourceTexts(messages: Map<String, LocalizedString>) {
        sourceTexts = messages
        merge()
    }

    /** Vain testejä varten: palauttaa luokkapolun lähdetekstit voimaan. */
    fun reloadSourceTexts() = setSourceTexts(readSourceTexts())

    private fun merge() {
        merged =
            if (sourceTexts.isEmpty()) {
                store
            } else {
                (sourceTexts.keys + store.keys).associateWith { key ->
                    val elava = store[key]
                    val lahde = sourceTexts[key]
                    when {
                        lahde == null -> {
                            elava!!
                        }

                        elava == null -> {
                            lahde
                        }

                        else -> {
                            LocalizedString(
                                fi = elava.fi ?: lahde.fi,
                                sv = elava.sv ?: lahde.sv,
                                en = elava.en ?: lahde.en,
                            )
                        }
                    }
                }
            }
    }

    private fun readSourceTexts(): Map<String, LocalizedString> =
        Language.entries.associateWith { readLocale(it) }.yhdistaKaannokset()

    private fun readLocale(language: Language): Map<String, String> {
        val locale = language.name.lowercase()
        val resource = ClassPathResource("$KAANNOSHAKEMISTO/$locale.json")
        if (!resource.exists()) return emptyMap()
        return runCatching {
            resource.inputStream.use { defaultObjectMapper.readValue<Map<String, String>>(it) }
        }.getOrElse {
            logger.error("Käännöstiedoston $locale.json lukeminen epäonnistui", it)
            emptyMap()
        }
    }
}
