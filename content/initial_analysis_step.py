import pandas as pd
import sys
from content.utils.data_processing import load_csv_data, set_index

columnsToErase = [
  'fecha_aprobación',
  'tipo_subsidio',
  'numero_garaje_1',
  'matricula_garaje_1',
  'numero_garaje_2',
  'matricula_garaje_2',
  'numero_garaje_3',
  'matricula_garaje_3',
  'numero_garaje_4',
  'matricula_garaje_4',
  'numero_garaje_5',
  'matricula_garaje_5',
  'numero_deposito_1',
  'matricula_inmobiliaria_deposito_1',
  'numero_deposito_2',
  'matricula_inmobiliaria_deposito_2',
  'numero_deposito_3',
  'matricula_inmobiliaria_deposito_3',
  'numero_deposito_4',
  'matricula_inmobiliaria_deposito_4',
  'numero_deposito_5',
  'matricula_inmobiliaria_deposito_5',
  'metodo_valuacion_1',
  'concepto_del_metodo_1',
  'metodo_valuacion_2',
  'concepto_del_metodo_2',
  'metodo_valuacion_3',
  'concepto_del_metodo_3',
  'metodo_valuacion_4',
  'concepto_del_metodo_4',
  'metodo_valuacion_5',
  'concepto_del_metodo_5',
  'metodo_valuacion_6',
  'concepto_del_metodo_6',
  'metodo_valuacion_7',
  'concepto_del_metodo_7',
  'metodo_valuacion_8',
  'concepto_del_metodo_8',
  'metodo_valuacion_9',
  'concepto_del_metodo_9',
  'Longitud',
  'Latitud',
  'descripcion_clase_inmueble',
  'perspectivas_de_valorizacion',
  'actualidad_edificadora',
  'comportamiento_oferta_demanda',
  'observaciones_generales_inmueble',
  'observaciones_estructura',
  'observaciones_generales_construccion',
  'observaciones_dependencias',
  'departamento_inmueble',
  'municipio_inmueble',
  'barrio',
  'descripcion_general_sector',
  'direccion_inmueble_informe',
  'descripcion_general_sector',
  'observaciones_generales_inmueble',
  'observaciones_estructura',
  'observaciones_dependencias',
  'observaciones_generales_construccion',
  'area_privada',
  'area_garaje',
  'area_deposito',
  'area_terreno',
  'area_construccion',
  'area_otros',
  'area_libre',
  'garaje_cubierto_1',
  'garaje_doble_1',
  'garaje_paralelo_1',
  'garaje_servidumbre_1',
  'garaje_cubierto_2',
  'garaje_doble_2',
  'garaje_paralelo_2',
  'garaje_servidumbre_2',
  'garaje_cubierto_3',
  'garaje_doble_3',
  'garaje_paralelo_3',
  'garaje_servidumbre_3',
  'garaje_cubierto_4',
  'garaje_doble_4',
  'garaje_paralelo_4',
  'garaje_servidumbre_4',
  'garaje_cubierto_5',
  'garaje_doble_5',
  'garaje_paralelo_5',
  'garaje_servidumbre_5',
  'garaje_visitantes',
  'altura_permitida',
  'observaciones_altura_permitida',
  'aislamiento_posterior',
  'observaciones_aislamiento_posterior',
  'aislamiento_lateral',
  'observaciones_aislamiento_lateral',
  'antejardin',
  'observaciones_antejardin',
  'indice_ocupacion',
  'observaciones_indice_ocupacion',
  'indice_construccion',
  'observaciones_indice_construccion',
  'predio_subdividido_fisicamente', # Si | No (Contiene datos espureos)
  'rph', # (Muchas, se nota tambien muchos datos espureos)
  'sometido_a_propiedad_horizontal',
  'condicion_ph',
  'ajustes_sismoresistentes', # No Disponibles | No Reparados | Reparados (Contiene datos espureos)
  'danos_previos',
  'valor_area_privada',
  'valor_area_garaje',
  'valor_area_deposito',
  'valor_area_terreno',
  'valor_area_construccion',
  'valor_area_otros',
  'valor_area_libre',
  'valor_uvr',
  'valor_avaluo_en_uvr',
]

categorical_columns = [
  'objeto', # Originación | Remate (Contiene datos espureos)
  'motivo', # Crédito hipotecario de vivienda | Empleados | Leasing Visto Bueno | Leasing Habitacional | Remates | Garantía | Actualizacion de garantias | Colomext Hipotecario | Credito Comercial | Compra de cartera | Dacion en Pago | Leasing Comercial | Reformas | Originacion | Leasing Inmobiliario - Persona Natural
  'proposito', # Crédito hipotecario de vivienda | Garantía Hipotecaria | Transaccion Comercial de Venta | Valor Asegurable
  'tipo_avaluo', # Hipotecario | Remates | Garantia Hipotecaria
  'tipo_credito', # Vivienda | Diferente de Vivienda | Hipotecario
  'sector', # Urbano | Rural | Poblado (Contiene datos espureos)
  'tipo_inmueble', # Apartamento | Casa | Casa Rural | Conjunto o Edificio | Deposito | Finca | Garaje | Lote | Lote Urbano | Oficina (Contiene datos espureos)
  'uso_actual', # (Muchas, se nota tambien muchos datos espureos)
  'clase_inmueble', # (Muchas, se nota tambien muchos datos espureos)
  'ocupante', # (Muchas, se nota tambien muchos datos espureos)
  'area_actividad', # (Muchas, se nota tambien muchos datos espureos)
  'uso_principal_ph', # Vivienda | Finca | Viviend, Serv y Comercio (Muchas, se nota tambien muchos datos espureos)
  'estructura', # Mamposteria Estructural | Tradicional | Industrializada | Muro de carga (Contiene datos espureos)
  'cubierta', # Teja Metalica | Teja Plastica | Tradicional | Teja fibrocemento | Teja de Barro (Contiene datos espureos)
  'fachada', # Concreto texturado | Flotante | Graniplast | Industrilizada | Ladrillo a la vista (Contiene datos espureos)
  'estructura_reforzada', # Flotante | Graniplast | Trabes coladas en sitio | No tiene trabes (Contiene datos espureos)
  'material_de_construccion', # Acero | Adobe, bahareque o tapia | Concreto Reforzado (Contiene datos espureos)
  'detalle_material', # Mampostería reforzada | Pórticos | Mampostería confinada (Contiene datos espureos)
  'iluminacion', # Bueno | Paneles prefabricados | Muros (Contiene datos espureos)
  'calidad_acabados_cocina', # Integral | Semi-Integral | Sencillo | Bueno | Lujoso | Normal | Regular | Sin Acabados
  'tipo_garaje', # Bueno | Comunal | Exclusivo | Integral | Lujoso | No tiene | Normal | Privado | Regular | Semi-Integral | Sencillo | Sin Acabados
]
binary_columns = [
  'alcantarillado_en_el_sector', # Si | No (Contiene datos espureos)
  'acueducto_en_el_sector', # Si | No (Contiene datos espureos)
  'gas_en_el_sector', # Si | No
  'energia_en_el_sector', # Si | No
  'telefono_en_el_sector', # Si | No
  'vias_pavimentadas', # Si | No
  'sardineles_en_las_vias', # Si | No
  'andenes_en_las_vias', # Si | No
  'barrio_legal', # Si | No (Contiene datos espureos)
  'paradero', # Si | No (Contiene datos espureos)
  'alumbrado', # Si | No (Contiene datos espureos)
  'arborizacion', # Si | No (Contiene datos espureos)
  'alamedas', # Si | No
  'ciclo_rutas', # Si | No
  'alcantarillado_en_el_predio', # Si | No (Contiene datos espureos)
  'acueducto_en_el_predio', # Si | No (Contiene datos espureos)
  'gas_en_el_predio', # Si | No (Contiene datos espureos)
  'energia_en_el_predio', # Si | No (Contiene datos espureos)
  'telefono_en_el_predio', # Si | No (Contiene datos espureos)
  'porteria', # Si | No (Contiene datos espureos)
  'citofono', # Si | No (Contiene datos espureos)
  'bicicletero', # Si | No (Contiene datos espureos)
  'piscina', # Si | No (Contiene datos espureos)
  'tanque_de_agua', # Si | No (Contiene datos espureos)
  'club_house', # Si | No (Contiene dato espureo "0", podria tomarse como No)
  'teatrino', # Si | No (Contiene dato espureo "0", podria tomarse como No)
  'sauna', # Si | No (Contiene dato espureo "0", podria tomarse como No)
  'vigilancia_privada', # Si | No (Contiene dato espureo "0", podria tomarse como No)
  'administracion', # Si | No (Contiene datos espureos)
]
ordinal_columns = [
  'estrato', # 1 - 6 (Contiene datos espureos)
  'topografia_sector', # Inclinado | Ligera | Plano (Contiene datos espureos)
  'condiciones_salubridad', # Buenas | Malas | Regulares (Contiene datos espureos)
  'transporte', # Bueno | Regular | Malo (Contiene datos espureos)
  'demanda_interes', # Nula | Bueno | Debil | Fuerte (Contiene datos espureos)
  'nivel_equipamiento_comercial', # En Proyecto | Regular Malo | Bueno | Muy bueno (Contiene datos espureos)
  'tipo_vigilancia', # 12 Horas | 24 Horas | No (Dato espureo Si podria tomarse como "24 Horas", dato espureo "0" podria tomarse como "No")
  'tipo_fachada', # De 0 a 3 metros | de 3 a 6 metros | Mayor a 6 metros (Contiene datos espureos)
  'ventilacion', # Bueno | Regular | Malo (Contiene datos espureos)
  'irregularidad_planta', # Sin irregularidad | No disponible | Con irregularidad (Contiene datos espureos)
  'irregularidad_altura', # Sin irregularidad | No disponible | Con irregularidad (Contiene datos espureos)=
  'estado_acabados_cocina', # Bueno | Lujoso | Malo | Normal | Regular | Sencillo | Sin acabados
  'estado_acabados_pisos', # Bueno | Sin Acabados | Normal | Sencillo (Contiene datos espureos)
  'calidad_acabados_pisos', # Bueno | Sin Acabados | Normal | Sencillo (Contiene datos espureos)
  'estado_acabados_muros', # Bueno | Sin Acabados | Normal | Sencillo (Contiene datos espureos)
  'calidad_acabados_muros', # Bueno | Sin Acabados | Normal | Sencillo (Contiene datos espureos)
  'estado_acabados_techos', # Bueno | Sin Acabados | Normal | Sencillo (Contiene datos espureos)
  'calidad_acabados_techos', # Bueno | Sin Acabados | Normal | Sencillo (Contiene datos espureos)
  'estado_acabados_madera', # Bueno | Sin Acabados | Normal | Sencillo (Contiene datos espureos)
  'calidad_acabados_madera', # Bueno | Lujoso | Malo | Normal | Regular | Sencillo | Sin acabados
  'estado_acabados_metal', # Bueno | Lujoso | Malo | Normal | Regular | Sencillo | Sin acabados
  'calidad_acabados_metal', # Bueno | Lujoso | Malo | Normal | Regular | Sencillo | Sin acabados
  'estado_acabados_banos', # Bueno | Lujoso | Malo | Normal | Regular | Sencillo | Sin acabados
  'calidad_acabados_banos', # Bueno | Lujoso | Malo | Normal | Regular | Sencillo | Sin acabados
]
numeric_columns = [
  # Seccion Informacion del inmueble
  'unidades', # [Int] 0 - 92 (Contiene datos espureos)
  'contadores_agua', # [Int] 0 - 6 (Contiene datos espureos "92", "Aplica", "No", podria asumirse que es cero, "Resultante")
  'contadores_luz', # [Int] 0 - 6 (Contiene datos espureos "92", "Aplica", "No", podria asumirse que es cero)
  'accesorios', # # [Int] 0 - 46 (Contiene dato espureo "No", podria asumirse que es cero)
  'area_valorada', # [Float] 0.0 - 1058.2 (Contiene unos numeros gitantes)
  'numero_piso', # [Int] 0 - 99 (Contiene datos espureos)
  'numero_de_edificios', # [Int] 0 - 99 (Contiene datos espureos)
  'vetustez', # a veces dice anhos antiguedad, a veces el anho de construccion
  'pisos_bodega', # [Int] 0 - 52 (Contiene datos espureos)
  'habitaciones', # [Int] 0 - 32 (Contiene datos espureos)
  'estar_habitacion', # [Int] 0 - 9 (Contiene datos espureos)
  'cuarto_servicio', # [Int] 0 - 5 (Contiene datos espureos)
  'closet', # [Int] 0 - 17 (Contiene datos espureos)
  'sala', # [Int] 0 - 24 (Contiene datos espureos)
  'comedor', # [Int] 0 - 31 (Contiene datos espureos)
  'bano_privado', # [Int] 0 - 24 (Contiene datos espureos)
  'bano_social', # [Int] 0 - 12
  'bano_servicio', # [Int] 0 - 11
  'cocina', # [Int] 0 - 13
  'estudio', # [Int] 0 - 3
  'balcon', # [Int] 0 - 11
  'terraza', # [Int] 0 - 9
  'patio_interior', # [Int] 0 - 11
  'jardin', # [Int] 0 - 4
  'zona_de_ropas', # [Int] 0 - 13
  'zona_verde_privada', # [Int] 0 - 4
  'local', # [Int] 0 - 10
  'oficina', # [Int] 0 - 9
  'bodega', # [Int] 0 - 2
  # Seccion Garage
  'numero_total_de_garajes', # [Int] 0 - 5 (Contiene datos espureos)
  'total_cupos_parquedaro', # [Int] 0 - 8 (Contiene datos espureos)
]


def get_clean_data (df = "./content/sample_data/train.csv"):
  try:
    rawData = load_csv_data(df)
    indexedData = set_index(rawData)
    cleanData = indexedData.drop(columnsToErase, axis=1)

    cleanData[numeric_columns] = cleanData[numeric_columns].fillna(0)
    for column in numeric_columns:
      cleanData[column] = cleanData[column].str.replace(",", ".")

    cleanData[numeric_columns] = cleanData.loc[:,numeric_columns].transform(lambda x: x.map(lambda x: { "Si": 1., "No": 0. }.get(x,x)))

    cleanData[numeric_columns] = cleanData[numeric_columns].apply(pd.to_numeric).astype(float)
    cleanData[binary_columns] = cleanData.loc[:,binary_columns].transform(lambda x: x.map(lambda x: { "Si": 1., "No": 0. }.get(x,x)))
    cleanData[binary_columns] = cleanData[binary_columns].fillna(0.).apply(pd.to_numeric).astype(float)
    cleanData['estrato'] = cleanData.loc[:,'estrato'].transform(lambda x: x.map(lambda x: { "Comercial": 7., "Oficina": 8., "Industrial": 9. }.get(x,x)))

    cleanData['topografia_sector'] = cleanData.loc[:,'topografia_sector'].transform(lambda x: x.map(lambda x: { "Plano": 0., "Ligera": 1., "Inclinado": 2., "Accidentada": 3. }.get(x,x)))

    cleanData['condiciones_salubridad'] = cleanData.loc[:,'condiciones_salubridad'].transform(lambda x: x.map(lambda x: { "Malas": 0., "Bueno": 1., "Buenas": 1., "Regulares": 2., "Malas": 3. }.get(x,x)))

    cleanData['transporte'] = cleanData.loc[:,'transporte'].transform(lambda x: x.map(lambda x: { "Malo": 0., "Regular": 1., "Bueno": 2., "Vivienda": 3., "Hotelero": 4. }.get(x,x)))

    cleanData['demanda_interes'] = cleanData.loc[:,'demanda_interes'].transform(lambda x: x.map(lambda x: { "Nula": 0., "Débil": 1., "Media": 2., "Bueno": 3., "Fuerte": 4. }.get(x,x)))

    cleanData['nivel_equipamiento_comercial'] = cleanData.loc[:,'nivel_equipamiento_comercial'].transform(lambda x: x.map(lambda x: { "En Proyecto": 1., "Regular Malo": 0., "Bueno": 2., "Muy bueno": 3. }.get(x,x)))

    cleanData['tipo_vigilancia'] = cleanData.loc[:,'tipo_vigilancia'].transform(lambda x: x.map(lambda x: { "12 Horas": 1., "24 Horas": 2. }.get(x,x)))

    cleanData['tipo_fachada'] = cleanData.loc[:,'tipo_fachada'].transform(lambda x: x.map(lambda x: { "De 0 a 3 metros": 1., "De 3 a 6 metros": 2., "Mayor a 6 metros": 3. }.get(x,x)))

    cleanData['ventilacion'] = cleanData.loc[:,'ventilacion'].transform(lambda x: x.map(lambda x: { "Malo": 0., "Regular": 1., "Bueno": 2. }.get(x,x)))

    cleanData['irregularidad_planta'] = cleanData.loc[:,'irregularidad_planta'].transform(lambda x: x.map(lambda x: { "No disponible": 0., "Con irregularidad": 1., "Sin irregularidad": 2. }.get(x,x)))

    cleanData['irregularidad_altura'] = cleanData.loc[:,'irregularidad_altura'].transform(lambda x: x.map(lambda x: { "No disponible": 0., "Con irregularidad": 1., "Sin irregularidad": 2. }.get(x,x)))

    dictionary_details = { "Malo": 0., "Sin Acabados": 1., "Sin acabados": 1., "Sencillo": 2., "Normal": 4., "Bueno": 5., "Lujoso": 5., "No disponible": 0., "Regular": 3,}

    cleanData['estado_acabados_cocina'] = cleanData.loc[:,'estado_acabados_cocina'].transform(lambda x: x.map(lambda x: dictionary_details.get(x,x)))

    cleanData['estado_acabados_pisos'] = cleanData.loc[:,'estado_acabados_pisos'].transform(lambda x: x.map(lambda x: dictionary_details.get(x,x)))

    cleanData['calidad_acabados_pisos'] = cleanData.loc[:,'calidad_acabados_pisos'].transform(lambda x: x.map(lambda x: dictionary_details.get(x,x)))

    cleanData['estado_acabados_muros'] = cleanData.loc[:,'estado_acabados_muros'].transform(lambda x: x.map(lambda x: dictionary_details.get(x,x)))

    cleanData['calidad_acabados_muros'] = cleanData.loc[:,'calidad_acabados_muros'].transform(lambda x: x.map(lambda x: dictionary_details.get(x,x)))

    cleanData['estado_acabados_techos'] = cleanData.loc[:,'estado_acabados_techos'].transform(lambda x: x.map(lambda x: dictionary_details.get(x,x)))

    cleanData['calidad_acabados_techos'] = cleanData.loc[:,'calidad_acabados_techos'].transform(lambda x: x.map(lambda x: dictionary_details.get(x,x)))

    cleanData['estado_acabados_madera'] = cleanData.loc[:,'estado_acabados_madera'].transform(lambda x: x.map(lambda x: dictionary_details.get(x,x)))

    cleanData['calidad_acabados_madera'] = cleanData.loc[:,'calidad_acabados_madera'].transform(lambda x: x.map(lambda x: dictionary_details.get(x,x)))

    cleanData['estado_acabados_metal'] = cleanData.loc[:,'estado_acabados_metal'].transform(lambda x: x.map(lambda x: dictionary_details.get(x,x)))

    cleanData['calidad_acabados_metal'] = cleanData.loc[:,'calidad_acabados_metal'].transform(lambda x: x.map(lambda x: dictionary_details.get(x,x)))

    cleanData['estado_acabados_banos'] = cleanData.loc[:,'estado_acabados_banos'].transform(lambda x: x.map(lambda x: dictionary_details.get(x,x)))

    cleanData['calidad_acabados_banos'] = cleanData.loc[:,'calidad_acabados_banos'].transform(lambda x: x.map(lambda x: dictionary_details.get(x,x)))


    cleanData[ordinal_columns] = cleanData[ordinal_columns].fillna(0.).apply(pd.to_numeric,errors='coerce').astype(float)
    cleanData = pd.get_dummies(cleanData,
                        columns = categorical_columns,
                        dtype=float
                        )

    return cleanData
  except Exception as e:
    print(e)
    sys.exit(1)