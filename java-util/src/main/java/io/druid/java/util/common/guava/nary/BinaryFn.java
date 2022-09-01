/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.java.util.common.guava.nary;

/**
 */
public interface BinaryFn<Type1, Type2, OutType>
{
  OutType apply(Type1 arg1, Type2 arg2);

  interface Identical<T> extends BinaryFn<T, T, T>
  {
    default T done(T ret) {return ret;}
  }
}
