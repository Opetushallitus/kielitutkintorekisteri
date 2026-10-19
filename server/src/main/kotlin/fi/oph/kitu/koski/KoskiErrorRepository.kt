package fi.oph.kitu.koski

import fi.oph.kitu.auditlogs.AuditLogger
import fi.oph.kitu.koodisto.Koodisto
import fi.oph.kitu.util.toJsonNode
import fi.oph.kitu.vkt.CustomVktSuoritusRepository
import org.springframework.data.annotation.Id
import org.springframework.data.jdbc.repository.query.Modifying
import org.springframework.data.jdbc.repository.query.Query
import org.springframework.data.relational.core.mapping.Table
import org.springframework.data.repository.CrudRepository
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import org.springframework.stereotype.Service
import tools.jackson.databind.JsonNode
import java.time.Instant

@Repository
interface KoskiErrorRepository : CrudRepository<KoskiErrorEntity, String> {
    @Modifying
    @Query(
        """
        INSERT INTO koski_error(id, entity, message, timestamp)
        VALUES (:id, :entity, :message, now())
        ON CONFLICT (id, entity) DO UPDATE 
        SET timestamp = now(), message = :message
    """,
    )
    fun upsert(
        @Param("id") id: String,
        @Param("entity") entity: String,
        @Param("message") message: String,
    )

    fun find(id: KoskiErrorMappingId): KoskiErrorEntity? = findByIdAndEntity(id.mappedId(), id.entityName)

    fun findByIdAndEntity(
        id: String,
        entityId: String,
    ): KoskiErrorEntity?

    fun delete(id: KoskiErrorMappingId) = deleteByIdAndEntity(id.mappedId(), id.entityName)

    fun deleteByIdAndEntity(
        id: String,
        entity: String,
    )

    fun findAllByEntityAndHidden(
        entity: String,
        hidden: Boolean,
    ): List<KoskiErrorEntity>

    fun countByEntityAndHidden(
        entity: String,
        hidden: Boolean,
    ): Int

    @Modifying
    @Query(
        """
        UPDATE koski_error
        SET hidden = :hidden
        WHERE id = :id AND entity = :entity
    """,
    )
    fun setHidden(
        @Param("id") id: String,
        @Param("entity") entity: String,
        @Param("hidden") hidden: Boolean,
    )
}

@Table(name = "koski_error")
data class KoskiErrorEntity(
    @Id
    val id: String,
    val entity: String,
    val message: String,
    val timestamp: Instant,
    val hidden: Boolean,
) {
    fun errorJson(): JsonNode? {
        val matchResult = Regex("^[\\w\\s]+:\\s\"(.*)\"$").find(message)
        return matchResult?.let {
            matchResult.groupValues[1].toJsonNode()
        }
    }
}

@Service
class KoskiErrorService(
    val repository: KoskiErrorRepository,
    private val auditLogger: AuditLogger,
) {
    fun save(
        id: KoskiErrorMappingId,
        message: String,
    ) {
        repository.upsert(
            id.mappedId(),
            id.entityName,
            message = message,
        )
    }

    fun setHidden(
        id: KoskiErrorMappingId,
        hidden: Boolean,
    ) {
        repository.setHidden(id.mappedId(), id.entityName, hidden)
    }

    fun findById(id: KoskiErrorMappingId): KoskiErrorEntity? =
        repository
            .find(id)
            ?.also {
                auditLogger.logAllInternalOnly("Koski ${it.entity}-export error viewed", listOf(it)) { error ->
                    arrayOf("koski.error.id" to error.id)
                }
            }

    fun reset(id: KoskiErrorMappingId) = repository.delete(id)

    fun findAllByEntity(
        entity: String,
        hidden: Boolean,
    ) = repository
        .findAllByEntityAndHidden(entity, hidden)
        .also {
            auditLogger.logAllInternalOnly("Koski $entity-export error viewed", it) { error ->
                arrayOf("koski.error.id" to error.id)
            }
        }

    fun countByEntity(
        entity: String,
        hidden: Boolean,
    ) = repository.countByEntityAndHidden(entity, hidden)
}

sealed class KoskiErrorMappingId(
    val entityName: String,
) {
    abstract fun mappedId(): String
}

data class VktMappingId(
    val ryhma: CustomVktSuoritusRepository.Tutkintoryhma,
) : KoskiErrorMappingId(ENTITY) {
    override fun mappedId(): String = "${ryhma.oppijanumero}/${ryhma.tutkintokieli.name}/${ryhma.taitotaso.name}"

    companion object {
        const val ENTITY = "vkt"

        fun parse(id: String): VktMappingId? {
            try {
                val (oppijanumero, kieli, taitotaso) = id.split("/")
                return VktMappingId(
                    CustomVktSuoritusRepository.Tutkintoryhma(
                        oppijanumero = oppijanumero,
                        tutkintokieli = Koodisto.Tutkintokieli.valueOf(kieli),
                        taitotaso = Koodisto.VktTaitotaso.valueOf(taitotaso),
                    ),
                )
            } catch (_: Throwable) {
                return null
            }
        }
    }
}

data class YkiMappingId(
    val suoritusId: Int?,
) : KoskiErrorMappingId(ENTITY) {
    override fun mappedId(): String = suoritusId.toString()

    companion object {
        const val ENTITY = "yki"

        fun parse(id: String): YkiMappingId? =
            try {
                YkiMappingId(
                    suoritusId = id.toInt(),
                )
            } catch (_: Throwable) {
                null
            }
    }
}
