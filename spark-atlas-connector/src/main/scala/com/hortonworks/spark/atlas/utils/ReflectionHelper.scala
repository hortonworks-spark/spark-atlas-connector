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

  def reflectFieldWithContextClassloaderLoosenType(obj: Any, fieldName: String): Option[Any] = {
    val typeMirror = runtimeMirror(Thread.currentThread().getContextClassLoader)
    val instanceMirror = typeMirror.reflect(obj)

    val members = instanceMirror.symbol.typeSignature.members
    val field = members.find(_.name.decodedName.toString == fieldName)
    field match {
      case Some(f) =>
        try {
          Some(instanceMirror.reflectField(f.asTerm).get)
        } catch {
          case NonFatal(e) =>
            logWarn(s"Failed to reflect field $fieldName from $obj. " +
              s"Maybe missing to apply necessary patch? $e")
            None
        }

      case None =>
        logWarn(s"Failed to reflect field $fieldName from $obj. " +
          s"Maybe missing to apply necessary patch?")
        None
    }
  }

  def reflectFieldWithContextClassloader[OUT](obj: Any, fieldName: String): Option[OUT] = {
    reflectFieldWithContextClassloaderLoosenType(obj, fieldName).map(_.asInstanceOf[OUT])
  }

  def reflectMethodWithContextClassloaderLoosenType(
      obj: Any,
      methodName: String,
      params: Any*): Option[Any] = {
    val typeMirror = runtimeMirror(Thread.currentThread().getContextClassLoader)
    val instanceMirror = typeMirror.reflect(obj)

    val members = instanceMirror.symbol.typeSignature.members
    val method = members.find(_.name.decodedName.toString == methodName)
    method match {
      case Some(f) =>
        try {
          Some(instanceMirror.reflectMethod(f.asMethod).apply(params))
        } catch {
          case NonFatal(_) =>
            logWarn(s"Failed to call method $methodName from $obj via reflection. " +
              s"Maybe missing to apply necessary patch?")
            None
        }

      case None =>
        logWarn(s"Failed to call method $methodName from $obj via reflection. " +
          s"Maybe missing to apply necessary patch?")
        None
    }
  }

  def reflectMethodWithContextClassloader[OUT](
      obj: Any,
      fieldName: String,
      params: Any*): Option[OUT] = {
    reflectMethodWithContextClassloaderLoosenType(obj, fieldName, params: _*)
      .map(_.asInstanceOf[OUT])
  }

  def classForName(className: String): Class[_] = {
    Class.forName(className, true, getContextOrClassClassLoader)
  }

  private def getContextOrClassClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader)
}
