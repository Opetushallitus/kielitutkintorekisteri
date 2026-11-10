package fi.oph.kitu.koski

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.tiedontuontischema.VktHenkilosuoritus
import fi.oph.kitu.util.TimeService
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.util.result.splitIntoValuesAndErrors
import fi.oph.kitu.vkt.CustomVktSuoritusRepository
import fi.oph.kitu.vkt.VktSuoritusService
import fi.oph.kitu.yki.suoritukset.YkiSuoritusEntity
import fi.oph.kitu.yki.suoritukset.YkiSuoritusRepository
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Service
import org.springframework.web.client.RestClientException
import java.time.LocalDateTime

@Service
class KoskiService(
    private val koskiClient: KoskiClient,
    private val koskiYkiRequestMapper: KoskiYkiRequestMapper,
    private val koskiVktRequestMapper: KoskiVktRequestMapper,
    private val ykiSuoritusRepository: YkiSuoritusRepository,
    private val customVktSuoritusRepository: CustomVktSuoritusRepository,
    private val vktSuoritusService: VktSuoritusService,
    private val koskiErrors: KoskiErrorService,
    private val timeService: TimeService,
    @param:Value($$"${kitu.koski.yki.transferBlockedUntil:#{null}}")
    private val ykiTransferBlockedUntil: LocalDateTime? = null,
) {
    @WithSpan
    fun sendYkiSuoritusToKoski(ykiSuoritusEntity: YkiSuoritusEntity): Either<KoskiException, YkiSuoritusEntity> {
        val koskiRequest =
            when (val result = koskiYkiRequestMapper.ykiSuoritusToKoskiRequest(ykiSuoritusEntity)) {
                is Either.Right -> {
                    result.value
                }

                is Either.Left -> {
                    when (val error = result.value) {
                        is KoskiYkiMappingError.EstoSyyt -> {
                            val suoritus = ykiSuoritusEntity.copy(koskiSiirtoKasitelty = true)
                            ykiSuoritusRepository.save(suoritus, updateOnConflict = true, forceWrite = true)
                            return suoritus.right()
                        }

                        is KoskiYkiMappingError.InvalidArvosana -> {
                            return KoskiValidationException(
                                YkiMappingId(ykiSuoritusEntity.solkiId),
                                "Suorituksen muuntaminen Koski-pyynnöksi epäonnistui: ${error.cause.message}",
                            ).left()
                        }

                        is KoskiYkiMappingError.InvalidKoodistoValue -> {
                            return KoskiValidationException(
                                YkiMappingId(ykiSuoritusEntity.solkiId),
                                "Suorituksen muuntaminen Koski-pyynnöksi epäonnistui: ${error.cause.message}",
                            ).left()
                        }
                    }
                }
            }

        val koskiResponse =
            when (val result = putToKoski(YkiMappingId(ykiSuoritusEntity.solkiId), koskiRequest)) {
                is Either.Right -> result.value
                is Either.Left -> return result.value.left()
            }

        val koskiOpiskeluoikeus =
            koskiResponse.body
                ?.opiskeluoikeudet
                ?.firstOrNull()
                ?.oid
                ?.let { Oid.parse(it).getOrNull() }
                ?: return KoskiException(
                    YkiMappingId(ykiSuoritusEntity.solkiId),
                    "KOSKI-opiskeluoikeuden OID puuttuu vastauksesta tai on virheellinen",
                ).left()

        val suoritus =
            ykiSuoritusEntity.copy(
                koskiOpiskeluoikeus = koskiOpiskeluoikeus,
                koskiSiirtoKasitelty = true,
            )
        ykiSuoritusRepository.save(suoritus, updateOnConflict = true, forceWrite = true)
        return suoritus.right()
    }

    @WithSpan
    fun sendVktSuoritusToKoski(suoritus: VktHenkilosuoritus): Either<KoskiException, Unit> {
        val id = CustomVktSuoritusRepository.Tutkintoryhma.from(suoritus)
        val koskiRequest = koskiVktRequestMapper.vktSuoritusToKoskiRequest(suoritus)
        if (koskiRequest.isLeft()) {
            // Suoritus ei ole vielä valmis lähetettäväksi, mutta se ei ole tiedonsiirtovirhe.
            return Unit.right()
        }

        val koskiResponse =
            when (val result = putToKoski(VktMappingId(id), koskiRequest.getOrThrow())) {
                is Either.Right -> result.value
                is Either.Left -> return result.value.left()
            }

        val koskiOpiskeluoikeusOid =
            koskiResponse.body
                ?.opiskeluoikeudet
                ?.firstOrNull()
                ?.oid

        vktSuoritusService.markKoskiTransferProcessed(suoritus, koskiOpiskeluoikeusOid)
        return Unit.right()
    }

    @WithSpan
    fun sendYkiSuorituksetToKoski(): Either<KoskiTechnicalException, KoskiTransferReport> {
        val transferBlocked = isYkiTransferBlocked()
        val suoritukset =
            ykiSuoritusRepository
                .findKoskeenLahettamattomatSuoritukset()
                .filter { !transferBlocked || it.isOphTesti() }
        val results =
            suoritukset.map { suoritus ->
                Either
                    .catch { sendYkiSuoritusToKoski(suoritus) }
                    .fold(
                        ifLeft = { KoskiTechnicalException(YkiMappingId(suoritus.solkiId), it.toString()).left() },
                        ifRight = { it },
                    )
            }
        return reportErrors(
            results.map { it.map { suoritus -> YkiMappingId(suoritus.solkiId) } },
            blockedUntil = ykiTransferBlockedUntil.takeIf { transferBlocked },
        )
    }

    private fun isYkiTransferBlocked(): Boolean {
        val blockedUntil = ykiTransferBlockedUntil ?: return false
        return timeService.now().isBefore(blockedUntil.atZone(TimeService.zoneId).toInstant())
    }

    @WithSpan
    fun sendVktSuorituksetToKoski(): Either<KoskiTechnicalException, KoskiTransferReport> {
        val siirrettavat = customVktSuoritusRepository.findOpiskeluoikeudetForKoskiTransfer()
        val results =
            siirrettavat.map { id ->
                Either
                    .catch {
                        vktSuoritusService.getOppijanSuoritukset(id, false)?.let { suoritus ->
                            sendVktSuoritusToKoski(suoritus).map {
                                VktMappingId(CustomVktSuoritusRepository.Tutkintoryhma.from(suoritus))
                            }
                        }
                    }.fold(
                        ifLeft = { KoskiTechnicalException(VktMappingId(id), it.toString()).left() },
                        ifRight = { it },
                    )
            }
        return reportErrors(results)
    }

    private fun putToKoski(
        id: KoskiErrorMappingId,
        koskiRequest: KoskiRequest,
    ): Either<KoskiException, ResponseEntity<KoskiResponse>> =
        try {
            koskiClient.put(koskiRequest).right()
        } catch (e: RestClientException) {
            KoskiException.from(id, e).left()
        }

    private inline fun <reified T : KoskiErrorMappingId> reportErrors(
        results: List<Either<KoskiException, T>?>,
        blockedUntil: LocalDateTime? = null,
    ): Either<KoskiTechnicalException, KoskiTransferReport> {
        val (success, failed) = results.filterNotNull().splitIntoValuesAndErrors()
        success.forEach { id -> koskiErrors.reset(id) }
        failed.forEach { error -> koskiErrors.save(error.suoritusId, error.message ?: error.toString()) }
        failed.forEach {
            when (it) {
                is KoskiTechnicalException -> return it.left()
                else -> Unit
            }
        }
        return KoskiTransferReport(
            success.size,
            results.size,
            blockedUntil = blockedUntil,
        ).right()
    }
}

data class KoskiTransferReport(
    val successfulTransfers: Int,
    val totalCount: Int,
    val timestamp: LocalDateTime = LocalDateTime.now(),
    val blockedUntil: LocalDateTime? = null,
) {
    override fun toString(): String {
        val eraajo =
            listOf(
                "Viimeisin onnistunut eräajo: $timestamp",
                "Siirretty $successfulTransfers / $totalCount",
            ).joinToString("; ")
        return if (blockedUntil != null) {
            "KOSKI-lähetys estetty $blockedUntil asti; $eraajo"
        } else {
            eraajo
        }
    }
}
