package fi.oph.kitu.yki.arvioijat.solki

import fi.oph.kitu.oppijanumero.OppijanumeroService
import fi.oph.kitu.yki.arvioijat.ArvioijarekisteriAsetukset
import fi.oph.kitu.yki.arvioijat.YkiArvioijaKausiRepository
import fi.oph.kitu.yki.arvioijat.YkiArvioijaPoistoRepository
import fi.oph.kitu.yki.arvioijat.YkiArvioijaRepository
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

/**
 * Toteutuksia on vain yksi, ja integraatiokytkin luetaan sen sisalla: kytkin estaa automaattisen
 * lahetyksen, mutta virkailijan kasin kaynnistama lahetys toimii aina.
 *
 * Bean rakennetaan tassa eika `@Service`-annotaatiolla, jotta [SolkiArvioijaServiceImpl] pysyy
 * tavallisena luokkana, jonka testit voivat konstruoida suoraan.
 */
@Configuration
class SolkiArvioijaConfig {
    @Bean
    fun solkiArvioijaLahetys(
        repository: YkiArvioijaRepository,
        poistoRepository: YkiArvioijaPoistoRepository,
        kausiRepository: YkiArvioijaKausiRepository,
        client: SolkiArvioijaClient,
        oppijanumeroService: OppijanumeroService,
        asetukset: ArvioijarekisteriAsetukset,
    ): SolkiArvioijaService =
        SolkiArvioijaServiceImpl(
            repository,
            poistoRepository,
            kausiRepository,
            client,
            oppijanumeroService,
            asetukset,
        )
}
