package org.greenplum.pxf.plugins.clickhouse.utils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;
import java.util.Map;

/*
 * json 反序列化工具
 */
@SuppressWarnings("deprecation")
public class JackSonUtils {

    static Logger logger = LoggerFactory.getLogger(JackSonUtils.class);
    private static ObjectMapper m = new ObjectMapper();

    static {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        m.setDateFormat(format);
    }

    private JackSonUtils() {
    }

    public static ObjectMapper getM() {
        return m;
    }

    public static <T> T toBean(String jsonAsString, Class<T> pojoClass) {
        try {
            if (StringUtils.isEmpty(jsonAsString))
                return null;
            return m.readValue(jsonAsString, pojoClass);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    /**
     * 转换json字符串 不包括类型信息,如果转换出现异常返回空字符串
     */
    public static final String toJsonString(Object obj) {
        String json = "";
        try {
            json = m.writeValueAsString(obj);
        } catch (Exception e) {
            logger.error(obj.getClass().getName() + "对象转换为json出错！", e);
        }
        return json;
    }

    public static final byte[] toBytes(Object obj) {
        byte[] json = null;
        try {
            json = m.writeValueAsBytes(obj);
        } catch (Exception e) {
            logger.error(obj.getClass().getName() + "对象转换为字节出错！", e);
        }
        return json;
    }


    public static <T> Map<String, T> toMap(String jsonAsString) {
        try {
            return m.readValue(jsonAsString, new TypeReference<Map<String, T>>() {
            });
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    @JsonCreator
    public static <T> LinkedHashMap<String, T> toLinkedHashMap(byte[] jsonBytes) {
        try {
            return m.readValue(jsonBytes, new TypeReference<LinkedHashMap<String, T>>() {
            });
        } catch (Exception e) {
            try {
                String msg = new String(jsonBytes, "UTF-8");
                logger.error(msg, e);
            } catch (UnsupportedEncodingException e1) {
                logger.error("byte convert String error", e1);
            }
            return null;
        }
    }

    @JsonCreator
    public static <T> LinkedHashMap<String, T> toLinkedHashMap(String jsonAsString) {
        try {
            return m.readValue(jsonAsString, new TypeReference<LinkedHashMap<String, T>>() {
            });
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    public static <T> Map<String, T>[] toMapArray(String jsonAsString) {
        try {
            return m.readValue(jsonAsString, new TypeReference<Map<String, T>[]>() {
            });
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    public static <T> T fileToJson(InputStream in, TypeReference<T> typeReference) {
        try {
            return m.readValue(in, typeReference);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    public static <T> T jsonToClass(String in, TypeReference<T> typeReference) {
        try {
            return m.readValue(in, typeReference);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    public static final JsonNode toJsonNode(String json) {
        if (StringUtils.isEmpty(json)) return null;
        JsonNode node = null;
        try {
            node = m.readTree(json);
        } catch (IOException e) {
            logger.error("json字符串：" + json + ", 转换jsonNode对象出错！", e);
        }
        return node;
    }

}
