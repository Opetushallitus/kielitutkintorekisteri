package fi.oph.kitu.kotoutumiskoulutus.koealusta.tehtavapankki

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import io.opentelemetry.api.trace.Span
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.stereotype.Service
import tools.jackson.core.StreamReadConstraints
import tools.jackson.databind.DeserializationFeature
import tools.jackson.databind.JsonNode
import tools.jackson.dataformat.xml.XmlFactory
import tools.jackson.dataformat.xml.XmlMapper
import java.io.InputStream

@Service
class TehtavapankkiXmlParser {
    // Tehtäväpankit sisältävät base64-upotettuja mediatiedostoja, jotka voivat
    // ylittää Jacksonin oletusrajan (20 MB). Nostetaan lataus­polun rajan tasalle.
    private val xmlMapper: XmlMapper =
        XmlMapper
            .builder(
                XmlFactory
                    .builder()
                    .streamReadConstraints(
                        StreamReadConstraints.builder().maxStringLength(200_000_000).build(),
                    ).build(),
            ).disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build()

    @WithSpan
    fun parse(input: InputStream): Either<TehtavapankkiParseError, TehtavapankkiQuiz> =
        try {
            val tree = xmlMapper.readTree(input)
            val quiz = buildQuiz(tree)
            Span.current().setAttribute("questionCount", quiz.questions.size.toLong())
            quiz.right()
        } catch (e: Throwable) {
            TehtavapankkiParseError.InvalidXml(e).left()
        }

    fun parse(xml: String): Either<TehtavapankkiParseError, TehtavapankkiQuiz> =
        parse(xml.byteInputStream(Charsets.UTF_8))

    private fun buildQuiz(root: JsonNode): TehtavapankkiQuiz {
        val raw = root.get("question") ?: return TehtavapankkiQuiz()
        val questions = raw.iterableNodes().map { buildQuestion(it) }
        return TehtavapankkiQuiz(questions)
    }

    private fun buildQuestion(node: JsonNode): Question {
        val type = node.attrString("type") ?: return UnknownQuestion(type = "")
        return when (type) {
            "category" -> {
                CategoryQuestion(
                    category = node.formattedText("category"),
                    info = node.formattedText("info"),
                    idnumber = node.childString("idnumber"),
                )
            }

            "description" -> {
                DescriptionQuestion(
                    name = node.formattedText("name"),
                    questiontext = node.richText("questiontext"),
                    generalfeedback = node.formattedText("generalfeedback"),
                    defaultgrade = node.childDouble("defaultgrade"),
                    penalty = node.childDouble("penalty"),
                    hidden = node.childInt("hidden"),
                    idnumber = node.childString("idnumber"),
                )
            }

            "multichoice" -> {
                MultichoiceQuestion(
                    name = node.formattedText("name"),
                    questiontext = node.richText("questiontext"),
                    generalfeedback = node.formattedText("generalfeedback"),
                    defaultgrade = node.childDouble("defaultgrade"),
                    penalty = node.childDouble("penalty"),
                    hidden = node.childInt("hidden"),
                    idnumber = node.childString("idnumber"),
                    single = node.childBoolean("single"),
                    shuffleanswers = node.childBoolean("shuffleanswers"),
                    answernumbering = node.childString("answernumbering"),
                    showstandardinstruction = node.childInt("showstandardinstruction"),
                    correctfeedback = node.formattedText("correctfeedback"),
                    partiallycorrectfeedback = node.formattedText("partiallycorrectfeedback"),
                    incorrectfeedback = node.formattedText("incorrectfeedback"),
                    answers = node.answers(),
                )
            }

            "shortanswer" -> {
                ShortanswerQuestion(
                    name = node.formattedText("name"),
                    questiontext = node.richText("questiontext"),
                    generalfeedback = node.formattedText("generalfeedback"),
                    defaultgrade = node.childDouble("defaultgrade"),
                    penalty = node.childDouble("penalty"),
                    hidden = node.childInt("hidden"),
                    idnumber = node.childString("idnumber"),
                    usecase = node.childInt("usecase"),
                    answers = node.answers(),
                )
            }

            "essay" -> {
                EssayQuestion(
                    name = node.formattedText("name"),
                    questiontext = node.richText("questiontext"),
                    generalfeedback = node.formattedText("generalfeedback"),
                    defaultgrade = node.childDouble("defaultgrade"),
                    penalty = node.childDouble("penalty"),
                    hidden = node.childInt("hidden"),
                    idnumber = node.childString("idnumber"),
                    responseformat = node.childString("responseformat"),
                    responserequired = node.childInt("responserequired"),
                    responsefieldlines = node.childInt("responsefieldlines"),
                    minwordlimit = node.childString("minwordlimit"),
                    maxwordlimit = node.childString("maxwordlimit"),
                    attachments = node.childInt("attachments"),
                    attachmentsrequired = node.childInt("attachmentsrequired"),
                    maxbytes = node.childLong("maxbytes"),
                    filetypeslist = node.childString("filetypeslist"),
                    graderinfo = node.formattedText("graderinfo"),
                    responsetemplate = node.formattedText("responsetemplate"),
                )
            }

            "cloudpoodll" -> {
                CloudpoodllQuestion(
                    name = node.formattedText("name"),
                    questiontext = node.richText("questiontext"),
                    generalfeedback = node.formattedText("generalfeedback"),
                    defaultgrade = node.childDouble("defaultgrade"),
                    penalty = node.childDouble("penalty"),
                    hidden = node.childInt("hidden"),
                    idnumber = node.childString("idnumber"),
                    responseformat = node.childString("responseformat"),
                    graderinfo = node.formattedText("graderinfo"),
                    qresource = node.childString("qresource"),
                    language = node.childString("language"),
                    expiredays = node.childInt("expiredays"),
                    transcriber = node.childInt("transcriber"),
                    studentplayer = node.childInt("studentplayer"),
                    teacherplayer = node.childInt("teacherplayer"),
                    transcode = node.childInt("transcode"),
                    audioskin = node.childString("audioskin"),
                    videoskin = node.childString("videoskin"),
                    timelimit = node.childInt("timelimit"),
                    safesave = node.childInt("safesave"),
                    noaudiofilters = node.childInt("noaudiofilters"),
                    tags = node.tags(),
                )
            }

            else -> {
                UnknownQuestion(type = type)
            }
        }
    }
}

/**
 * Returns the text content of a child element, normalized.
 *
 * Jackson XML's tree mode represents:
 *   <foo>bar</foo>           as a TextNode "bar"
 *   <foo attr="x">bar</foo>  as an ObjectNode { "attr": "x", "": "bar" }
 *   <foo/>                   as a TextNode "" (or null)
 * This helper unwraps both cases to the inner text.
 */
private fun JsonNode?.toElementText(): String? {
    if (this == null || this.isNull) return null
    if (this.isValueNode) return this.asString()
    if (this.isObject) return this.get("")?.takeIf { it.isValueNode }?.asString()
    return null
}

private fun JsonNode.attrString(name: String): String? = this.get(name)?.takeIf { it.isValueNode }?.asString()

private fun JsonNode.childString(name: String): String? = this.get(name)?.toElementText()

private fun JsonNode.childInt(name: String): Int? = this.get(name)?.toElementText()?.toIntOrNull()

private fun JsonNode.childLong(name: String): Long? = this.get(name)?.toElementText()?.toLongOrNull()

private fun JsonNode.childDouble(name: String): Double? = this.get(name)?.toElementText()?.toDoubleOrNull()

private fun JsonNode.childBoolean(name: String): Boolean? = this.get(name)?.toElementText()?.toBooleanStrictOrNull()

private fun JsonNode.formattedText(name: String): FormattedText? {
    val n = this.get(name) ?: return null
    return FormattedText(
        format = n.attrString("format"),
        text = n.get("text").toElementText(),
    )
}

private fun JsonNode.richText(name: String): RichText? {
    val n = this.get(name) ?: return null
    val files = (n.get("file"))?.iterableNodes()?.map { it.embeddedFile() }.orEmpty()
    return RichText(
        format = n.attrString("format"),
        text = n.get("text").toElementText(),
        embeddedFiles = files,
    )
}

private fun JsonNode.embeddedFile(): EmbeddedFile =
    EmbeddedFile(
        name = this.attrString("name") ?: "",
        path = this.attrString("path") ?: "/",
        encoding = this.attrString("encoding") ?: "base64",
        content = this.get("")?.takeIf { it.isValueNode }?.asString() ?: "",
    )

private fun JsonNode.answers(): List<Answer> = (this.get("answer"))?.iterableNodes()?.map { it.answer() }.orEmpty()

private fun JsonNode.answer(): Answer =
    Answer(
        fraction = this.attrString("fraction")?.toDoubleOrNull() ?: 0.0,
        format = this.attrString("format"),
        text = this.get("text").toElementText(),
        feedback = this.formattedText("feedback"),
    )

private fun JsonNode.tags(): List<Tag> {
    val n = this.get("tags") ?: return emptyList()
    return (n.get("tag"))?.iterableNodes()?.map { Tag(text = it.get("text").toElementText()) }.orEmpty()
}

/**
 * Iterates a node as a list. Jackson XML represents:
 *   - a single repeating element as that element's node directly
 *   - multiple as an ArrayNode
 * Normalize to always return an Iterable<JsonNode>.
 */
private fun JsonNode.iterableNodes(): List<JsonNode> = if (this.isArray) this.toList() else listOf(this)
