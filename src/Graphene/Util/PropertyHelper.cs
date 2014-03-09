// Copyright 2013-2014 Boban Jose
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. 
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, 
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Specialized;
using System.Threading;
using System.Reflection;
using System.Diagnostics;

namespace Graphene.Util
{
    public static class PropertyHelper<T>
    {
        public static PropertyInfo GetProperty<TValue>(
            Expression<Func<T, TValue>> selector)
        {
            Expression body = selector;
            if (body is LambdaExpression)
            {
                body = ((LambdaExpression)body).Body;
            }
            switch (body.NodeType)
            {
                case ExpressionType.MemberAccess:
                    return (PropertyInfo)((MemberExpression)body).Member;
                case ExpressionType.Convert:
                case ExpressionType.ConvertChecked:
                    var exp = body as UnaryExpression;
                    if (exp == null)
                        throw new InvalidOperationException();
                    var me = ((exp != null) ? exp.Operand : null) as MemberExpression;
                    if (me == null)
                        throw new InvalidOperationException();
                    return (PropertyInfo)(me).Member;
                default:
                    throw new InvalidOperationException();
            }
        }
    }
}
