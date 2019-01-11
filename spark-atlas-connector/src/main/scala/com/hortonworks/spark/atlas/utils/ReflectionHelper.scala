/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.spark.atlas.utils

import scala.util.control.NonFatal

object ReflectionHelper extends Logging {
  import scala.reflect.runtime.universe.{TermName, runtimeMirror, typeOf, TypeTag}
  private val currentMirror = runtimeMirror(getClass.getClassLoader)

  def reflectField[T, OUT](obj: Any, fieldName: String)(implicit ttag: TypeTag[T]): Option[OUT] = {
    val relMirror = currentMirror.reflect(obj)

    try {
      val method = typeOf[T].decl(TermName(fieldName)).asTerm.accessed.asTerm

      Some(relMirror.reflectField(method).get.asInstanceOf[OUT])
    } catch {
      case NonFatal(_) =>
        logWarn(s"Failed to reflect field $fieldName from $obj. " +
          s"Maybe missing to apply necessary patch?")
        None
    }
  }
}
