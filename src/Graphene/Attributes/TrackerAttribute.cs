﻿// Copyright 2013-2014 Boban Jose
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. 
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, 
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Graphene.Attributes
{
    [AttributeUsage(AttributeTargets.Class)]
    public class TrackerAttribute : Attribute
    {        
        public TrackerAttribute(string name)
        {
            Name = name;
        }

        public string Name { get; private set; }

        public string DisplayName { get; set; }

        public string Description { get; set; }

        public string ToolTip { get; set; }
    }
}