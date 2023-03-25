package com.whh.gmall.realtime.app.func;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * ClassName: TransientSink
 * Package: com.whh.gmall.realtime.app.func
 * Description:
 *
 * @Author whh
 * @Create 2023/3/25 15:56
 * @Version 1.0
 */

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface TransientSink {
}
