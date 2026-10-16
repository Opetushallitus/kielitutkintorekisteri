package fi.oph.kitu.dev.mockdata

import fi.oph.kitu.oid.Oid
import fi.oph.kitu.oid.OidClass
import fi.oph.kitu.util.result.getOrThrow
import kotlin.random.Random

fun createOid(
    oidClass: OidClass,
    n: Long,
): Oid =
    Oid
        .parse("${oidClass.node}.${n.toString().padStart(11, '0')}")
        .getOrThrow()

fun generateOidOfClass(
    oidClass: OidClass,
    random: Random = Random,
): Oid = createOid(oidClass, (0..99999999999).random(random))

fun generateRandomOppijaOid(random: Random = Random): Oid = generateOidOfClass(OidClass.OPPIJA, random)

fun generateRandomUserOid(random: Random = Random): Oid = generateOidOfClass(OidClass.USER, random)

fun generateRandomOrganizationOid(random: Random = Random) = generateOidOfClass(OidClass.ORG, random)
