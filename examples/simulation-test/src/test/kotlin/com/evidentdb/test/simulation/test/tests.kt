package com.evidentdb.test.simulation.test

import io.cloudevents.CloudEvent
import io.micronaut.serde.ObjectMapper
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class Tests {
    @Test
    fun parseInputEvent() {
//        val json = "{" +
//                "\"specversion\" : \"1.0\"," +
//                "\"id\" : \"4dcd84ee-e253-5e7e-04ba-94bccb2e4f46\"," +
//                "\"source\" : \"org\"," +
//                "\"type\" : \"Lumos\"," +
//                "\"subject\" : \"Garrick Ollivander\"," +
//                "\"time\" : 1718398730390," +
//                "\"datacontenttype\" : \"application/json\"," +
//                "\"data\" : {\n" +
//                "    \"mollitia_molestias\" : \"Sirius Black\"\n" +
//                "  \n}" +
//                "}"
//        val event = ObjectMapper.getDefault().readValue(json, JsonEvent::class.java).toCloudEvent()
//        Assertions.assertInstanceOf(CloudEvent::class.java, event)
//        Assertions.assertEquals(
//            event.data!!.toBytes().toString(Charsets.UTF_8),
//            "{\"mollitia_molestias\":\"Sirius Black\"}"
//        )
    }
}
