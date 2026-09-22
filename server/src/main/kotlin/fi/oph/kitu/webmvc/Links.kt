package fi.oph.kitu.webmvc

import fi.oph.kitu.config.ApplicationProperties
import fi.oph.kitu.i18n.LokalisointiViewController
import fi.oph.kitu.koodisto.Koodisto
import fi.oph.kitu.kotoutumiskoulutus.KielitestiApiController
import fi.oph.kitu.kotoutumiskoulutus.KielitestiViewController
import fi.oph.kitu.kotoutumiskoulutus.koealusta.tehtavapankki.TehtavapankkiViewController
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.vkt.VktApiController
import fi.oph.kitu.vkt.VktViewController
import fi.oph.kitu.yki.YkiApiController
import fi.oph.kitu.yki.YkiViewController
import fi.oph.kitu.yki.arvioijat.ArvioijaHakuFormData
import fi.oph.kitu.yki.arvioijat.KausiFormData
import fi.oph.kitu.yki.arvioijat.YkiArvioijaKausiViewController
import fi.oph.kitu.yki.arvioijat.YkiArvioijaParams
import fi.oph.kitu.yki.arvioijat.YkiArvioijaViewController
import org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.linkTo
import org.springframework.hateoas.server.mvc.WebMvcLinkBuilder.methodOn

object Links {
    fun home(): String = linkTo(methodOn(HomeController::class.java).home(null)).toString()

    object Dashboard {
        fun yki(): String = linkTo(methodOn(HomeController::class.java).ykiCard()).toString()

        fun vkt(): String = linkTo(methodOn(HomeController::class.java).vktCard()).toString()

        fun koto(): String = linkTo(methodOn(HomeController::class.java).kotoCard()).toString()

        fun admin(): String = linkTo(methodOn(HomeController::class.java).adminCard()).toString()
    }

    object Admin {
        fun dbScheduler(): String = "${ApplicationProperties.kitu.appUrl}/db-scheduler"

        fun tyhjennaKaannosvalimuisti(): String =
            linkTo(methodOn(LokalisointiViewController::class.java).tyhjennaValimuisti(null)).toString()
    }

    object Vkt {
        fun suoritukset(): String = linkTo(methodOn(VktViewController::class.java).suorituksetView()).toString()

        fun erinomaisenTaitotasonIlmoittautuneet(): String =
            linkTo(methodOn(VktViewController::class.java).erinomaisenTaitotasonIlmoittautuneetView()).toString()

        fun erinomaisenTaitotasonArvioidutSuoritukset(): String =
            linkTo(methodOn(VktViewController::class.java).erinomaisenTaitotasonArvioidutSuorituksetView()).toString()

        fun hyvanJaTyydyttavanTaitotasonSuoritukset(): String =
            linkTo(methodOn(VktViewController::class.java).hyvanJaTyydyttavanTaitotasonSuorituksetView()).toString()

        fun ilmoittautuneenArviointi(
            oppijanumero: String,
            kieli: Koodisto.Tutkintokieli,
            taso: Koodisto.VktTaitotaso,
        ): String =
            linkTo(
                methodOn(VktViewController::class.java).ilmoittautuneenArviointiView(oppijanumero, kieli, taso),
            ).toString()

        fun koskiVirheet(): String = linkTo(methodOn(VktViewController::class.java).showKoskiVirheet()).toString()

        fun koskiRequestJson(
            oppijanumero: String,
            kieli: Koodisto.Tutkintokieli,
            taso: Koodisto.VktTaitotaso,
        ): String =
            linkTo(
                methodOn(VktViewController::class.java).koskiRequestJson(oppijanumero, kieli, taso),
            ).toString()

        fun hideKoskiVirheet(
            oppijanumero: String,
            tutkintokieli: Koodisto.Tutkintokieli,
            taitotaso: Koodisto.VktTaitotaso,
            hidden: Boolean,
        ): String =
            linkTo(
                methodOn(
                    VktViewController::class.java,
                ).hideKoskiVirheet(oppijanumero, tutkintokieli, taitotaso, hidden),
            ).toString()

        fun suorituksetCsv(): String = linkTo(methodOn(VktApiController::class.java).getSuorituksetCsv()).toString()
    }

    object Yki {
        fun suoritukset(): String = linkTo(methodOn(YkiViewController::class.java).suorituksetGetView()).toString()

        fun suoritus(id: Int): String = "${ApplicationProperties.kitu.appUrl}/yki/suoritukset/$id"

        fun arvioijat(): String =
            linkTo(methodOn(YkiArvioijaViewController::class.java).arvioijatView(YkiArvioijaParams())).toString()

        fun arvioija(id: Int): String =
            linkTo(methodOn(YkiArvioijaViewController::class.java).arvioijaView(id, null)).toString()

        fun uusiArvioija(): String =
            linkTo(
                methodOn(YkiArvioijaViewController::class.java).uusiArvioijaView(ArvioijaHakuFormData()),
            ).toString()

        fun passivoiArvioija(id: Int): String =
            linkTo(methodOn(YkiArvioijaViewController::class.java).passivoiArvioija(id, null)).toString()

        fun lahetaArvioijaSolkiin(id: Int): String =
            linkTo(methodOn(YkiArvioijaViewController::class.java).lahetaArvioijaSolkiin(id, null)).toString()

        fun kaudet(id: Int): String =
            linkTo(
                methodOn(YkiArvioijaKausiViewController::class.java).luoKausi(id, KausiFormData(), null),
            ).toString()

        fun uusiKausi(id: Int): String =
            linkTo(
                methodOn(YkiArvioijaKausiViewController::class.java).uusiKausiView(id, KausiFormData()),
            ).toString()

        fun kausi(
            id: Int,
            kausiId: Int,
        ): String =
            linkTo(
                methodOn(YkiArvioijaKausiViewController::class.java).paivitaKausi(id, kausiId, KausiFormData(), null),
            ).toString()

        fun muokkaaKautta(
            id: Int,
            kausiId: Int,
        ): String =
            linkTo(
                methodOn(YkiArvioijaKausiViewController::class.java).muokkaaKausiView(id, kausiId),
            ).toString()

        fun passivoiKausi(
            id: Int,
            kausiId: Int,
        ): String =
            linkTo(
                methodOn(YkiArvioijaKausiViewController::class.java).passivoiKausi(id, kausiId, null),
            ).toString()

        fun poistaKausi(
            id: Int,
            kausiId: Int,
        ): String =
            linkTo(
                methodOn(YkiArvioijaKausiViewController::class.java).poistaKausi(id, kausiId, null),
            ).toString()

        fun muokkaaArvioijaa(id: Int): String =
            linkTo(methodOn(YkiArvioijaViewController::class.java).muokkaaArvioijaaView(id)).toString()

        fun arvioijaHaku(): String =
            linkTo(
                methodOn(YkiArvioijaViewController::class.java).arvioijaHaku(ArvioijaHakuFormData()),
            ).toString()

        fun tarkistusArvioinnit(): String =
            linkTo(methodOn(YkiViewController::class.java).tarkistusArvioinnitView()).toString()

        fun hyvaksytytTarkistusArvioinnit(): String =
            linkTo(methodOn(YkiViewController::class.java).hyvaksytytTarkistusArvioinnitView()).toString()

        fun suorituksetVirheet(): String =
            linkTo(methodOn(YkiViewController::class.java).suorituksetVirheetView()).toString()

        fun arvioijatCsv(): String =
            linkTo(methodOn(YkiApiController::class.java).getArvioijatCsv(YkiArvioijaParams())).toString()

        fun koskiVirheet(): String = linkTo(methodOn(YkiViewController::class.java).koskiVirheetView()).toString()

        fun koskiRequestJson(suoritusId: Int): String =
            linkTo(methodOn(YkiViewController::class.java).koskiRequestJson(suoritusId)).toString()

        fun hideKoskiVirheet(
            suoritusId: Int,
            hidden: Boolean,
        ): String = linkTo(methodOn(YkiViewController::class.java).hideKoskiVirheet(suoritusId, hidden)).toString()

        fun suorituksetCsv(): String = linkTo(methodOn(YkiApiController::class.java).getSuorituksetAsCsv()).toString()
    }

    object Kielitesti {
        fun suoritukset(): String = linkTo(methodOn(KielitestiViewController::class.java).suorituksetView()).toString()

        fun suoritus(id: Int): String =
            linkTo(methodOn(KielitestiViewController::class.java).suoritusView(id)).toString()

        fun virheet(): String = linkTo(methodOn(KielitestiViewController::class.java).virheetView()).toString()

        fun suorituksetCsv(): String =
            linkTo(methodOn(KielitestiApiController::class.java).getSuorituksetAsCsv()).toString()

        fun virheetCsv(): String = linkTo(methodOn(KielitestiApiController::class.java).getErrorsAsCsv()).toString()
    }

    object Tehtavapankki {
        fun list(): String = linkTo(methodOn(TehtavapankkiViewController::class.java).listView()).toString()

        fun paketti(id: Int): String =
            linkTo(methodOn(TehtavapankkiViewController::class.java).pakettiView(id)).toString()

        fun download(s3Avain: String): String =
            linkTo(methodOn(TehtavapankkiViewController::class.java).downloadRedirect(s3Avain)).toString()
    }

    object Opintopolku {
        fun onr(oid: Oid): String = "/henkilo-ui/oppija/$oid"
    }
}
