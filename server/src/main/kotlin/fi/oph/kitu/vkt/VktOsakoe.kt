package fi.oph.kitu.vkt

import com.fasterxml.jackson.annotation.JsonSubTypes
import fi.oph.kitu.koodisto.Koodisto
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.tiedontuontischema.Arviointi
import fi.oph.kitu.tiedontuontischema.Arvioitava
import fi.oph.kitu.tiedontuontischema.Osasuoritus
import java.time.Instant
import java.time.LocalDate

@JsonSubTypes(
    JsonSubTypes.Type(value = VktKirjoittamisenKoe::class, name = "kirjoittaminen"),
    JsonSubTypes.Type(value = VktTekstinYmmartamisenKoe::class, name = "tekstinymmartaminen"),
    JsonSubTypes.Type(value = VktPuhumisenKoe::class, name = "puhuminen"),
    JsonSubTypes.Type(value = VktPuheenYmmartamisenKoe::class, name = "puheenymmartaminen"),
)
interface VktOsakoe :
    Osasuoritus,
    Arvioitava {
    val internalId: Int?
    override val tyyppi: Koodisto.VktOsakoe

    val tutkintopaiva: LocalDate
    override val arviointi: VktArvionti?
    val oppilaitos: Oid?
    val merkittyPoistettavaksi: Instant?

    /**
     * Suorituksen vastaanottajan oid. Tätä ei siirretä kielitutkintorekisteriin,
     * vaan se täytetään VktSuoritus-objektin arvon mukaan käyttöliittymää renderöitäessä.
     */
    val suorituksenVastaanottaja: String?

    /**
     * Suorituspaikkakunnan koodiarvo. Tätä ei siirretä kielitutkintorekisteriin,
     * vaan se täytetään VktSuoritus-objektin arvon mukaan käyttöliittymää renderöitäessä.
     */
    val suorituspaikkakunta: String?

    fun toVktOsakoeRow() =
        VktSuoritusEntity.VktOsakoe(
            tyyppi = tyyppi,
            tutkintopaiva = tutkintopaiva,
            arviointipaiva = arviointi?.paivamaara,
            arvosana = arviointi?.arvosana,
            merkittyPoistettavaksi = merkittyPoistettavaksi,
        )

    companion object {
        fun from(row: VktSuoritusEntity.VktOsakoe) =
            when (row.tyyppi) {
                Koodisto.VktOsakoe.Kirjoittaminen -> {
                    VktKirjoittamisenKoe(
                        row.tutkintopaiva,
                        VktArvionti.from(row),
                        row.id,
                        merkittyPoistettavaksi = row.merkittyPoistettavaksi,
                    )
                }

                Koodisto.VktOsakoe.TekstinYmmärtäminen -> {
                    VktTekstinYmmartamisenKoe(
                        row.tutkintopaiva,
                        VktArvionti.from(row),
                        row.id,
                        merkittyPoistettavaksi = row.merkittyPoistettavaksi,
                    )
                }

                Koodisto.VktOsakoe.Puhuminen -> {
                    VktPuhumisenKoe(
                        row.tutkintopaiva,
                        VktArvionti.from(row),
                        row.id,
                        merkittyPoistettavaksi = row.merkittyPoistettavaksi,
                    )
                }

                Koodisto.VktOsakoe.PuheenYmmärtäminen -> {
                    VktPuheenYmmartamisenKoe(
                        row.tutkintopaiva,
                        VktArvionti.from(row),
                        row.id,
                        merkittyPoistettavaksi = row.merkittyPoistettavaksi,
                    )
                }
            }
    }
}

interface VktKirjallisenKielitaidonKoe : VktOsakoe

interface VktSuullisenKielitaidonKoe : VktOsakoe

interface VktYmmartamisenKielitaidonKoe : VktOsakoe

data class VktKirjoittamisenKoe(
    override val tutkintopaiva: LocalDate,
    override val arviointi: VktArvionti? = null,
    override val internalId: Int? = null,
    override val oppilaitos: Oid? = null,
    override val merkittyPoistettavaksi: Instant? = null,
    override val suorituksenVastaanottaja: String? = null,
    override val suorituspaikkakunta: String? = null,
) : VktKirjallisenKielitaidonKoe {
    override val tyyppi: Koodisto.VktOsakoe = Koodisto.VktOsakoe.Kirjoittaminen
}

data class VktTekstinYmmartamisenKoe(
    override val tutkintopaiva: LocalDate,
    override val arviointi: VktArvionti? = null,
    override val internalId: Int? = null,
    override val oppilaitos: Oid? = null,
    override val merkittyPoistettavaksi: Instant? = null,
    override val suorituksenVastaanottaja: String? = null,
    override val suorituspaikkakunta: String? = null,
) : VktKirjallisenKielitaidonKoe,
    VktYmmartamisenKielitaidonKoe {
    override val tyyppi: Koodisto.VktOsakoe = Koodisto.VktOsakoe.TekstinYmmärtäminen
}

data class VktPuhumisenKoe(
    override val tutkintopaiva: LocalDate,
    override val arviointi: VktArvionti? = null,
    override val internalId: Int? = null,
    override val oppilaitos: Oid? = null,
    override val merkittyPoistettavaksi: Instant? = null,
    override val suorituksenVastaanottaja: String? = null,
    override val suorituspaikkakunta: String? = null,
) : VktSuullisenKielitaidonKoe {
    override val tyyppi: Koodisto.VktOsakoe = Koodisto.VktOsakoe.Puhuminen
}

data class VktPuheenYmmartamisenKoe(
    override val tutkintopaiva: LocalDate,
    override val arviointi: VktArvionti? = null,
    override val internalId: Int? = null,
    override val oppilaitos: Oid? = null,
    override val merkittyPoistettavaksi: Instant? = null,
    override val suorituksenVastaanottaja: String? = null,
    override val suorituspaikkakunta: String? = null,
) : VktSuullisenKielitaidonKoe,
    VktYmmartamisenKielitaidonKoe {
    override val tyyppi: Koodisto.VktOsakoe = Koodisto.VktOsakoe.PuheenYmmärtäminen
}

data class VktArvionti(
    override val arvosana: Koodisto.VktArvosana,
    override val paivamaara: LocalDate,
) : Arviointi,
    Comparable<VktArvionti> {
    override fun compareTo(other: VktArvionti): Int = Koodisto.ArvosanaKoodiviite.compare(this.arvosana, other.arvosana)

    companion object {
        fun from(row: VktSuoritusEntity.VktOsakoe) =
            if (row.arvosana != null &&
                row.arviointipaiva != null
            ) {
                VktArvionti(row.arvosana, row.arviointipaiva)
            } else {
                null
            }
    }
}
