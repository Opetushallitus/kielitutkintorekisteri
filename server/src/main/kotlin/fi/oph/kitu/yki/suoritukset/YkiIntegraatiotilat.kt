package fi.oph.kitu.yki.suoritukset

import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.koski.KoskiErrorEntity
import java.time.Instant

enum class IntegraationTila {
    ONNISTUI,
    EPAONNISTUI,
    ODOTTAA,
    EI_SIIRRETA,
}

data class Aikaleima(
    val nimi: LocalizedString?,
    val aika: Instant?,
)

data class Integraatio(
    val tila: IntegraationTila,
    val otsikko: LocalizedString,
    val aikaleima: Aikaleima? = null,
    val selitteet: List<LocalizedString> = emptyList(),
)

data class YkiIntegraatiotilat(
    val solki: Integraatio,
    val koski: Integraatio,
    val kios: Integraatio,
) {
    companion object {
        fun laske(
            suoritus: YkiSuoritusEntity,
            viimeisin: YkiSuoritusEntity,
            koskiError: KoskiErrorEntity?,
            koskiSiirronEstonSyyt: List<String>?,
        ) = YkiIntegraatiotilat(
            solki = solki(suoritus),
            koski = koski(suoritus, koskiError, koskiSiirronEstonSyyt),
            kios = kios(suoritus, viimeisin),
        )

        private fun solki(suoritus: YkiSuoritusEntity) =
            Integraatio(
                tila = IntegraationTila.ONNISTUI,
                otsikko = UiText.Yki.Integraatio.vastaanotettu,
                aikaleima = Aikaleima(UiText.Yki.Sarake.rekisteriintuontiaika, suoritus.receivedAt),
            )

        private fun koski(
            suoritus: YkiSuoritusEntity,
            koskiError: KoskiErrorEntity?,
            estonSyyt: List<String>?,
        ): Integraatio =
            when {
                !estonSyyt.isNullOrEmpty() -> {
                    Integraatio(
                        tila = IntegraationTila.EI_SIIRRETA,
                        otsikko = UiText.Yki.siirtoaEiTehda,
                        selitteet = estonSyyt.map { LocalizedString(fi = it) },
                    )
                }

                koskiError != null -> {
                    Integraatio(
                        tila = IntegraationTila.EPAONNISTUI,
                        otsikko = UiText.Yki.Integraatio.siirtoEpaonnistui,
                        aikaleima = Aikaleima(UiText.Yki.Integraatio.viimeisinYritys, koskiError.timestamp),
                        selitteet = listOf(UiText.Yki.Integraatio.yritetaanUudelleen),
                    )
                }

                suoritus.koskiSiirtoKasitelty == true -> {
                    Integraatio(
                        tila = IntegraationTila.ONNISTUI,
                        otsikko = UiText.Yki.Integraatio.siirretty,
                        aikaleima = Aikaleima(null, suoritus.koskiSiirretty),
                    )
                }

                else -> {
                    Integraatio(
                        tila = IntegraationTila.ODOTTAA,
                        otsikko = UiText.Yki.Integraatio.odottaaSiirtoa,
                    )
                }
            }

        private fun kios(
            suoritus: YkiSuoritusEntity,
            viimeisin: YkiSuoritusEntity,
        ): Integraatio {
            val eiLahetetaSyy =
                when {
                    suoritus.arviointitila.pelkkäIlmoittautuminen() -> UiText.Yki.suoritustaEdeltavaEiLaheteta
                    suoritus.isVilppi() -> UiText.Yki.Integraatio.vilppiaEiLaheteta
                    suoritus.suorittajanOID == null -> UiText.Yki.eiOppijanumeroa
                    else -> null
                }
            val lahetetty = suoritus.arviointitilaLahetetty?.toInstant()
            return when {
                eiLahetetaSyy != null -> {
                    Integraatio(
                        tila = IntegraationTila.EI_SIIRRETA,
                        otsikko = UiText.Yki.Integraatio.eiLaheteta,
                        selitteet = listOf(eiLahetetaSyy),
                    )
                }

                suoritus.arviointitilanLahetysvirhe != null -> {
                    Integraatio(
                        tila = IntegraationTila.EPAONNISTUI,
                        otsikko = UiText.Yki.Integraatio.lahetysEpaonnistui,
                        aikaleima =
                            Aikaleima(UiText.Yki.Integraatio.viimeisinYritys, suoritus.arviointitilanLahetysyritys),
                        selitteet = listOf(UiText.Yki.Integraatio.yritetaanUudelleen),
                    )
                }

                lahetetty != null && !lahetetty.isBefore(viimeisin.lastModified) -> {
                    Integraatio(
                        tila = IntegraationTila.ONNISTUI,
                        otsikko = UiText.Yki.arviointitilaLahetetty,
                        aikaleima = Aikaleima(null, lahetetty),
                    )
                }

                else -> {
                    Integraatio(
                        tila = IntegraationTila.ODOTTAA,
                        otsikko = UiText.Yki.Integraatio.odottaaLahetysta,
                        aikaleima = lahetetty?.let { Aikaleima(UiText.Yki.Integraatio.edellinenLahetys, it) },
                    )
                }
            }
        }
    }
}
