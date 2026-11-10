package fi.oph.kitu.vkt

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.dev.mockdata.VktSuoritusMockGenerator
import fi.oph.kitu.koodisto.Koodisto
import fi.oph.kitu.tiedontuontischema.VktValidation
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

private const val OPISKELUOIKEUS = "1.2.246.562.15.82831651349"

@SpringBootTest
@Import(DBContainerConfiguration::class)
class VktVersiointiTest(
    @param:Autowired private val suoritusRepository: VktSuoritusRepository,
    @param:Autowired private val customSuoritusRepository: CustomVktSuoritusRepository,
    @param:Autowired private val osakoeRepository: VktOsakoeRepository,
    @param:Autowired private val vktValidation: VktValidation,
) {
    private val generaattori = VktSuoritusMockGenerator()

    @BeforeEach
    fun tyhjenna() {
        suoritusRepository.deleteAll()
    }

    private fun kiosinLahettama(taso: Koodisto.VktTaitotaso): VktSuoritusEntity {
        val suoritus =
            generateSequence { generaattori.generateRandomVktSuoritusEntity(vktValidation) }
                .first { it.taitotaso == taso }
        return if (taso == Koodisto.VktTaitotaso.Erinomainen) {
            suoritus.copy(
                osakokeet = suoritus.osakokeet.map { it.copy(arvosana = null, arviointipaiva = null) }.toSet(),
                tutkinnot = suoritus.tutkinnot.map { it.copy(arvosana = null, arviointipaiva = null) }.toSet(),
            )
        } else {
            suoritus
        }
    }

    private fun VktSuoritusEntity.ryhma() =
        CustomVktSuoritusRepository.Tutkintoryhma(suorittajanOid.toString(), tutkintokieli, taitotaso)

    private fun versiot(suoritus: VktSuoritusEntity) =
        suoritusRepository.findByIlmoittautumisenId(suoritus.ilmoittautumisenId).sortedBy { it.createdAt }

    private fun siirretty(
        tallennettu: VktSuoritusEntity?,
        oid: String? = OPISKELUOIKEUS,
    ) {
        val suoritus = assertNotNull(tallennettu)
        customSuoritusRepository.markSuoritusTransferredToKoski(
            suoritus.ryhma(),
            suoritus.osakokeet.mapNotNull { it.id },
            oid,
        )
    }

    @Test
    fun `KOSKI-siirron jalkeen sama lahetys ei luo uutta versiota`() {
        val suoritus = kiosinLahettama(Koodisto.VktTaitotaso.HyväJaTyydyttävä)
        siirretty(customSuoritusRepository.save(suoritus))

        customSuoritusRepository.save(suoritus)

        val versiot = versiot(suoritus)
        assertEquals(1, versiot.size)
        assertEquals(OPISKELUOIKEUS, versiot.single().koskiOpiskeluoikeus.toString())
    }

    @Test
    fun `KOSKI-siirron jalkeen muuttunut lahetys sailyttaa opiskeluoikeuden ja menee uudelleen siirtoon`() {
        val suoritus = kiosinLahettama(Koodisto.VktTaitotaso.HyväJaTyydyttävä)
        siirretty(customSuoritusRepository.save(suoritus))

        customSuoritusRepository.save(suoritus.copy(sukunimi = suoritus.sukunimi + "-Korjattu"))

        val uusin = versiot(suoritus).last()
        assertEquals(suoritus.sukunimi + "-Korjattu", uusin.sukunimi)
        assertEquals(OPISKELUOIKEUS, uusin.koskiOpiskeluoikeus.toString())
        assertFalse(uusin.koskiSiirtoKasitelty)
    }

    @Test
    fun `virkailijan erinomaisen arvosana sailyy kun KIOS lahettaa ilmoittautumisen uudelleen`() {
        val suoritus = kiosinLahettama(Koodisto.VktTaitotaso.Erinomainen)
        val tallennettu = assertNotNull(customSuoritusRepository.save(suoritus))
        val osakoe = tallennettu.osakokeet.first()
        osakoeRepository.updateArvosana(osakoe.id!!, Koodisto.VktArvosana.Hylätty, LocalDate.of(2026, 10, 1))

        customSuoritusRepository.save(suoritus)
        customSuoritusRepository.save(suoritus.copy(etunimet = suoritus.etunimet + " Uusi"))

        val uusin = versiot(suoritus).last()
        val arvioitu = uusin.osakokeet.single { it.tyyppi == osakoe.tyyppi && it.tutkintopaiva == osakoe.tutkintopaiva }
        assertEquals(Koodisto.VktArvosana.Hylätty, arvioitu.arvosana)
        assertEquals(LocalDate.of(2026, 10, 1), arvioitu.arviointipaiva)
    }

    @Test
    fun `siirron aikana saapunut ilmoittautuminen jaa siirtojonoon`() {
        val lahetetty = kiosinLahettama(Koodisto.VktTaitotaso.HyväJaTyydyttävä)
        val tallennettu = customSuoritusRepository.save(lahetetty)
        val kesken =
            kiosinLahettama(Koodisto.VktTaitotaso.HyväJaTyydyttävä).let { uusi ->
                uusi.copy(
                    suorittajanOid = lahetetty.suorittajanOid,
                    tutkintokieli = lahetetty.tutkintokieli,
                    osakokeet =
                        uusi.osakokeet
                            .map {
                                it.copy(
                                    arvosana = it.arvosana ?: Koodisto.VktArvosana.Hyvä,
                                    arviointipaiva = it.arviointipaiva ?: it.tutkintopaiva,
                                )
                            }.toSet(),
                )
            }
        customSuoritusRepository.save(kesken)

        siirretty(tallennettu)

        assertTrue(versiot(lahetetty).single().koskiSiirtoKasitelty)
        assertFalse(versiot(kesken).single().koskiSiirtoKasitelty)
        assertTrue(customSuoritusRepository.findOpiskeluoikeudetForKoskiTransfer().contains(lahetetty.ryhma()))
    }

    @Test
    fun `vastaus ilman opiskeluoikeutta ei poista tallennettua`() {
        val suoritus = kiosinLahettama(Koodisto.VktTaitotaso.HyväJaTyydyttävä)
        val tallennettu = customSuoritusRepository.save(suoritus)
        siirretty(tallennettu)

        siirretty(tallennettu, oid = null)

        assertEquals(OPISKELUOIKEUS, versiot(suoritus).single().koskiOpiskeluoikeus.toString())
    }

    @Test
    fun `listaus ja laskenta rajaavat vain viimeisinta versiota`() {
        val suoritus = kiosinLahettama(Koodisto.VktTaitotaso.HyväJaTyydyttävä).copy(sukunimi = "Vanhanen")
        customSuoritusRepository.save(suoritus)
        customSuoritusRepository.save(suoritus.copy(sukunimi = "Uusinen"))

        val vanhalla = VktSuoritusFilter(search = "Vanhanen")
        val uudella = VktSuoritusFilter(search = "Uusinen")

        assertEquals(emptyList(), customSuoritusRepository.find(vanhalla, VktSuoritusOrder()).toList())
        assertEquals(0, customSuoritusRepository.numberOfRowsForListView(vanhalla))
        assertEquals(listOf("Uusinen"), customSuoritusRepository.find(uudella, VktSuoritusOrder()).map { it.sukunimi })
        assertEquals(1, customSuoritusRepository.numberOfRowsForListView(uudella))
    }

    @Test
    fun `KIOSin hyvan ja tyydyttavan arvosanan korjaus luo uuden version`() {
        val suoritus = kiosinLahettama(Koodisto.VktTaitotaso.HyväJaTyydyttävä)
        customSuoritusRepository.save(suoritus)
        val korjattava = suoritus.osakokeet.first { it.arvosana != null }
        val korjattu =
            korjattava.copy(
                arvosana =
                    if (korjattava.arvosana == Koodisto.VktArvosana.Hyvä) {
                        Koodisto.VktArvosana.Tyydyttävä
                    } else {
                        Koodisto.VktArvosana.Hyvä
                    },
            )

        customSuoritusRepository.save(suoritus.copy(osakokeet = suoritus.osakokeet - korjattava + korjattu))

        assertEquals(2, versiot(suoritus).size)
    }
}
