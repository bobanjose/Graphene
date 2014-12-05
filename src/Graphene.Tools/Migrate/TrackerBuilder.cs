using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Graphene.Tracking;

namespace Graphene.Tools.Migrate
{
    using System;
    using System.Reflection;
    using System.Reflection.Emit;

    public interface ITrackableWithSetter : ITrackable
    {
        string Name { set; get; }
        string Description { set; get; }

        Resolution MinResolution { set; get; }
    }

    public static class TrackerBuilder
    {
        public static ITrackable CreateTrackerFromTypeName(string typeName, string trackerName)
        {
            var tracker = createTrackerObject(typeName);
            var trackerInstance = (ITrackableWithSetter) Activator.CreateInstance(tracker);
            trackerInstance.Name = trackerName;
            return trackerInstance;
        }

        public static Object CreateFilterObject(string keyFilter)
        {
            var tb = GetTypeBuilder("GrapheneFilter");

            foreach (string filter in keyFilter.Split(','))
            {
                var nv = filter.Split(new[] { "::" }, StringSplitOptions.None);
                if (nv.Length == 2)
                {
                    AddProperty(tb, nv[0], typeof(string));
                }
            }

            Type objectType = tb.CreateType();
            var filterInstace = Activator.CreateInstance(objectType);

            foreach (var filter in keyFilter.Split(','))
            {
                var nv = filter.Split(new[] { "::" }, StringSplitOptions.None);
                if (nv.Length == 2)
                {
                    var prop = objectType.GetProperty(nv[0]);
                    prop.SetValue(filterInstace, nv[1], null);
                }
            }
            return filterInstace;
        }

        private static Type createTrackerObject(string typeName)
        {
            var typeBuilder = GetTypeBuilder(typeName);
            typeBuilder.AddInterfaceImplementation(typeof(ITrackableWithSetter));
            AddProperty(typeBuilder, "Name", typeof(string));
            AddProperty(typeBuilder, "Description", typeof(string));
            AddProperty(typeBuilder, "MinResolution", typeof(Resolution));
            
            Type objectType = typeBuilder.CreateType();
            return objectType;
        }

        private static TypeBuilder GetTypeBuilder(string typeName)
        {
            var assemblyName = new AssemblyName("Graphene.Tools");

            var assemblyBuilder = AppDomain.CurrentDomain.DefineDynamicAssembly(assemblyName, AssemblyBuilderAccess.Run);

            var moduleBuilder = assemblyBuilder.DefineDynamicModule(typeName);

            var tb = moduleBuilder.DefineType(typeName, TypeAttributes.Public | TypeAttributes.AutoClass | TypeAttributes.AnsiClass |
                                                            TypeAttributes.BeforeFieldInit, typeof(System.Object));
            return tb;
        }

        private static void AddProperty(TypeBuilder tb, string propertyName, Type propertyType)
        {
            var field = tb.DefineField("_" + propertyName, propertyType, FieldAttributes.Private);
            
            var property = tb.DefineProperty(propertyName, PropertyAttributes.HasDefault, propertyType, null);

            var propertyGetter = tb.DefineMethod("get_" + propertyName, MethodAttributes.Public | MethodAttributes.SpecialName |
                                                                              MethodAttributes.HideBySig | MethodAttributes.Virtual, propertyType, Type.EmptyTypes);
            var propertyGetterIl = propertyGetter.GetILGenerator();
            propertyGetterIl.Emit(OpCodes.Ldarg_0);
            propertyGetterIl.Emit(OpCodes.Ldfld, field);
            propertyGetterIl.Emit(OpCodes.Ret);

            var propertySetter = tb.DefineMethod("set_" + propertyName, MethodAttributes.Public | MethodAttributes.SpecialName |
                                                                MethodAttributes.HideBySig | MethodAttributes.Virtual, null, new [] { propertyType });
            var propertySetterIl = propertySetter.GetILGenerator();
            propertySetterIl.Emit(OpCodes.Ldarg_0);
            propertySetterIl.Emit(OpCodes.Ldarg_1);
            propertySetterIl.Emit(OpCodes.Stfld, field);
            propertySetterIl.Emit(OpCodes.Ret);

            property.SetGetMethod(propertyGetter);
            property.SetSetMethod(propertySetter);
        }
    }
}

