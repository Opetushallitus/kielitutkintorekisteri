package fi.oph.kitu.yki.arvioijat.solki

import fi.oph.kitu.oppijanumero.OppijanumeroService
import fi.oph.kitu.yki.arvioijat.ArvioijarekisteriAsetukset
import fi.oph.kitu.yki.arvioijat.YkiArvioijaRepository
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

/**
 * Toteutuksia on vain yksi, ja integraatiokytkin luetaan sen sisalla: kytkin estaa automaattisen
 * lahetyksen, mutta virkailijan kasin kaynnistama lahetys toimii aina. Aiemmin kytkin valitsi
 * kahden beanin valilta, jolloin lahetyskoneisto puuttui kokonaan kytkimen ollessa pois eika
 * painiketta voinut toteuttaa.
 *
 * Bean rakennetaan tassa eika `@Service`-annotaatiolla, jotta [SolkiArvioijaServiceImpl] pysyy
 * tavallisena luokkana, jonka testit voivat konstruoida suoraan.
 */
@Configuration
class SolkiArvioijaConfig {
    @Bean
    fun solkiArvioijaLahetys(
        repository: YkiArvioijaRepository,
        client: SolkiArvioijaClient,
        oppijanumeroService: OppijanumeroService,
        asetukset: ArvioijarekisteriAsetukset,
    ): SolkiArvioijaService = SolkiArvioijaServiceImpl(repository, client, oppijanumeroService, asetukset)
}
