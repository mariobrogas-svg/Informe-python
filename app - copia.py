import pandas as pd
import numpy as np
import pyodbc
import warnings
import os
import openpyxl
import streamlit as st

# 1. CONFIGURACION Y SUPRESION DE ADVERTENCIAS
warnings.filterwarnings('ignore', category=UserWarning)

PATH_EXCEL = r"O:\TALLERES 2\Proyectado de 6 meses.xlsx"
FECHA_FILTRO_BROGAS = '2024-12-01'
FECHA_FILTRO_ML = '2025-09-01'

def conectar_odbc(dsn):
    try:
        return pyodbc.connect(f"DSN={dsn};Uid=SYSDBA;Pwd=masterkey")
    except Exception as e:
        print(f"Error de conexion en DSN {dsn}: {e}")
        return None

# Conexiones Globales
db_brogas = conectar_odbc("BROGAS")
db_brogasml = conectar_odbc("BROGASML")

# --- FUNCIONES DE EXTRACCION ---

def get_proyectado_desde_tabla():
    """Busca y extrae datos de la TABLA 'PROYECTADO_2' de forma segura para Pylance."""
    if not os.path.exists(PATH_EXCEL):
        print(f"Archivo no encontrado en: {PATH_EXCEL}")
        return pd.DataFrame()

    wb = openpyxl.load_workbook(PATH_EXCEL, data_only=True)
    
    # Buscamos la tabla y la hoja de forma que Pylance esté seguro de que existen
    res_data = None
    
    for sheet in wb.worksheets:
        if "PROYECTADO_2" in sheet.tables:
            tbl = sheet.tables["PROYECTADO_2"]
            # Obtenemos el rango de celdas directamente
            table_range = sheet[tbl.ref]
            
            # Convertimos a lista de listas usando .value de forma segura
            res_data = []
            for row in table_range:
                # Forzamos que cada fila sea tratada como una secuencia de celdas
                res_data.append([cell.value for cell in row]) # type: ignore
            break

    # Si res_data sigue siendo None, es que no encontramos la tabla
    if res_data is None or len(res_data) < 2:
        print("Error: No se encontro la Tabla 'PROYECTADO_2' o esta vacia.")
        return pd.DataFrame()

    # Crear DataFrame: res_data[0] son las cabeceras, res_data[1:] son los datos
    df = pd.DataFrame(res_data[1:], columns=res_data[0])
    
    # Limpiar nombres de columnas y asegurar tipos numéricos
    df.columns = df.columns.astype(str).str.strip()
    
    meses = ['MES2', 'MES3', 'MES4']
    for m in meses:
        if m in df.columns:
            df[m] = pd.to_numeric(df[m], errors='coerce').fillna(0)
        else:
            df[m] = 0
    
    df['PEDIDO_PROYECTADO'] = df[meses].sum(axis=1)
    
    # Identificar columnas de Codigo y Descripción (por nombre o posición)
    col_cod = 'Codigo' if 'Codigo' in df.columns else (df.columns[0] if len(df.columns) > 0 else 'Codigo')
    col_des = 'Descripción' if 'Descripción' in df.columns else (df.columns[1] if len(df.columns) > 1 else 'Descripción')
    
    return df[[col_cod, col_des, 'PEDIDO_PROYECTADO']].rename(columns={col_cod: 'Codigo', col_des: 'Descripción'})

def get_articulos_3(conn):
    art = pd.read_sql("SELECT CODIGOARTICULO, CODIGOPARTICULAR, DESCRIPCION FROM ARTICULOS", conn)
    casilleros = pd.read_sql("SELECT CODIGOARTICULO, CODIGODEPOSITO, STOCKACTUAL FROM CASILLEROS", conn)
    deps = pd.read_sql("SELECT CODIGODEPOSITO, DESCRIPCION FROM DEPOSITOS", conn)
    
    df = art.merge(casilleros, on="CODIGOARTICULO", how="left")
    df = df.merge(deps, on="CODIGODEPOSITO", how="left")
    
    excluir = ["COMPRAS NC", "ECOMMERCE_FACTURACION", "SCRAP", "SERVICIO TECNICO", "SHOWROOM", "M. NO CONFORMES"]
    df = df[~df['DESCRIPCION_y'].isin(excluir)]
    
    res = df.groupby(['CODIGOPARTICULAR', 'DESCRIPCION_x']).agg({'STOCKACTUAL': 'sum'}).reset_index()
    return res.rename(columns={'DESCRIPCION_x': 'DESCRIPCION', 'STOCKACTUAL': 'STOCK'})

def get_cuerpo_comprobantes(conn):
    query = f"SELECT CODIGOPARTICULAR, CANTIDAD, CANTIDADREMITIDA, TIPOCOMPROBANTE FROM CUERPOCOMPROBANTES WHERE FECHAMODIFICACION > '{FECHA_FILTRO_BROGAS}'"
    df = pd.read_sql(query, conn)
    df = df[df['TIPOCOMPROBANTE'].isin(['FA', 'FB', 'FCA', 'FE'])]
    df['Cant Pendiente'] = df['CANTIDAD'] - df['CANTIDADREMITIDA']
    return df[df['Cant Pendiente'] > 0].groupby('CODIGOPARTICULAR')['Cant Pendiente'].sum().reset_index()

def get_op_2(conn):
    """
    Replica la lógica de la tabla OP (2):
    1. Une Cabeza con Cuerpo de Orden.
    2. Une con Artículos para obtener el CODIGOPARTICULAR.
    3. Une con Estados para filtrar los 'TERMINADO'.
    4. Filtra Anuladas.
    """
    query = """
    SELECT 
        A.CODIGOPARTICULAR, 
        CP.CANTIDAD,
        E.DESCRIPCION as ESTADO_DESC
    FROM PRODCABEZAORDEN H
    INNER JOIN PRODCUERPOORDEN CP ON H.CODIGOORDEN = CP.CODIGOORDEN
    INNER JOIN ARTICULOS A ON CP.CODIGOARTICULO = A.CODIGOARTICULO
    LEFT JOIN ESTADOSORDENPRODUCCION E ON H.CODIGOESTADOOP = E.CODIGOESTADOOP
    WHERE H.ANULADA = 0
    """
    
    # Ejecutar consulta
    df = pd.read_sql(query, conn)
    
    # Limpiar espacios y filtrar los que NO están terminados
    df['ESTADO_DESC'] = df['ESTADO_DESC'].fillna('').astype(str).str.strip().str.upper()
    df = df[df['ESTADO_DESC'] != 'TERMINADO']
    
    # Agrupar por el código que usas para el reporte
    res = df.groupby('CODIGOPARTICULAR')['CANTIDAD'].sum().reset_index()
    return res.rename(columns={'CANTIDAD': 'CANTIDAD EN PROD'})

def get_prod_cuerpo_4(conn):
    """Calcula insumos pendientes cruzando con ARTICULOS."""
    # Usamos comillas dobles para el alias para intentar forzar el nombre, 
    # pero Pandas suele recibir lo que el driver ODBC decide.
    query = """
    SELECT 
        A.CODIGOPARTICULAR, 
        CP.CANTIDAD,
        CP.CANTIDADENTREGADA
    FROM PRODCUERPOORDEN CP
    INNER JOIN ARTICULOS A ON CP.CODIGOARTICULO = A.CODIGOARTICULO
    """
    df = pd.read_sql(query, conn)
    
    # 1. Normalizamos los nombres de las columnas a MAYÚSCULAS para evitar errores
    df.columns = df.columns.str.upper()
    
    # 2. Realizamos el cálculo directamente en Pandas para más seguridad
    # Usamos .get() por si acaso el campo se llama distinto (ej. CANTIDADRECIBIDA)
    cantidad = df.get('CANTIDAD', 0)
    entregada = df.get('CANTIDADENTREGADA', 0)
    
    df['RESTA_CALCULADA'] = cantidad - entregada
    
    # 3. Agrupamos usando los nombres que ya normalizamos
    res = df.groupby('CODIGOPARTICULAR')['RESTA_CALCULADA'].sum().reset_index()
    
    return res.rename(columns={'RESTA_CALCULADA': 'INSUMOS_PEND'})

def get_cuerpo_comprobantes_ml(conn):
    """Calcula pendientes en BROGASML normalizando nombres de columnas."""
    query = f"""
    SELECT CODIGOPARTICULAR, (CANTIDAD - CANTIDADREMITIDA) as RESTA 
    FROM CUERPOCOMPROBANTES 
    WHERE FECHAMODIFICACION > '{FECHA_FILTRO_ML}'
    """
    df = pd.read_sql(query, conn)
    
    # Normalizamos a mayúsculas para que no importe cómo lo devuelva el driver
    df.columns = df.columns.str.upper()
    
    if 'RESTA' in df.columns:
        return df.groupby('CODIGOPARTICULAR')['RESTA'].sum().reset_index().rename(columns={'RESTA': 'PENDIENTE_ML'})
    else:
        # Si por algún motivo no hay datos o la columna se llama distinto
        return pd.DataFrame(columns=['CODIGOPARTICULAR', 'PENDIENTE_ML'])

def get_pedidos_pendientes(conn):
    """
    Replica la lógica de CUERPOPEDIDOS y CABEZAPEDIDOS:
    Filtra pedidos no anulados, sin cancelar, sin remitir y sin preparar
    en depósitos EXPEDICION y FIZBAY.
    """
    query = """
    SELECT 
        CP.CODIGOPARTICULAR,
        CP.CANTIDAD,
        D.DESCRIPCION as DEPOSITO
    FROM CUERPOPEDIDOS CP
    INNER JOIN CABEZAPEDIDOS CB ON CP.NUMEROCOMPROBANTE = CB.NUMEROCOMPROBANTE 
                               AND CP.TIPOCOMPROBANTE = CB.TIPOCOMPROBANTE
    INNER JOIN DEPOSITOS D ON CP.CODIGODEPOSITO = D.CODIGODEPOSITO
    WHERE CB.ANULADA = 0 
      AND CP.CANTIDADCANCELADA = 0
      AND CP.CANTIDADREMITIDA = 0
      AND CP.CANTIDADPREPARADA = 0
      AND D.DESCRIPCION IN ('EXPEDICION', 'FIZBAY')
    """
    df = pd.read_sql(query, conn)
    if df.empty:
        return pd.DataFrame(columns=['CODIGOPARTICULAR', 'PEDIDOS_NUEVOS'])
    
    # Agrupamos por código para sumar las cantidades
    df.columns = df.columns.str.upper()
    res = df.groupby('CODIGOPARTICULAR')['CANTIDAD'].sum().reset_index()
    return res.rename(columns={'CANTIDAD': 'PEDIDOS_NUEVOS'})

# --- PROCESO PRINCIPAL ---

# --- PROCESO PRINCIPAL DE UNIÓN ---

print("--- Iniciando consolidacion final ---")

# Configuración de la página (Debe ser lo primero)
st.set_page_config(page_title="Monitor de Stock Brogas", layout="wide")

if db_brogas and db_brogasml:
    # Extracción de todas las fuentes
    df_proy = get_proyectado_desde_tabla()
    df_art = get_articulos_3(db_brogas)
    df_ventas = get_cuerpo_comprobantes(db_brogas)
    df_op = get_op_2(db_brogas)
    df_insumos = get_prod_cuerpo_4(db_brogas)
    df_ml = get_cuerpo_comprobantes_ml(db_brogasml)

    # NORMALIZACIÓN MASIVA: Aseguramos que todas las tablas tengan 
    # la columna de unión 'CODIGOPARTICULAR' en el mismo formato.
    listado_dfs = [df_proy, df_art, df_ventas, df_op, df_insumos, df_ml]
    for d in listado_dfs:
        if not d.empty:
            d.columns = d.columns.str.upper()
            if 'CODIGO' in d.columns and 'CODIGOPARTICULAR' not in d.columns:
                d.rename(columns={'CODIGO': 'CODIGOPARTICULAR'}, inplace=True)

    # UNIÓN SUCESIVA (MERGE)
    # Empezamos con el proyectado como base
    final = df_proy.copy()
    
    # Unimos cada tabla por la columna común
    for d in [df_art, df_ventas, df_ml, df_op, df_insumos]:
        if not d.empty:
            final = final.merge(d, on="CODIGOPARTICULAR", how="left")

    # RELLENAR NULOS Y CÁLCULOS FINALES
    final = final.fillna(0)
    
    # Usamos los nombres en mayúsculas que generó la normalización
    final['PENDIENTE_TOTAL'] = final.get('CANT PENDIENTE', 0) + final.get('PENDIENTE_ML', 0)
    final['STOCK_NETO'] = final.get('STOCK', 0) - final['PENDIENTE_TOTAL']
    
    # Cobertura
    with np.errstate(divide='ignore', invalid='ignore'):
        final['COBERTURA_MESES'] = (final['STOCK_NETO'] / final['PEDIDO_PROYECTADO']) * 3
        final['COBERTURA_MESES'] = final['COBERTURA_MESES'].replace([np.inf, -np.inf], 0).fillna(0)

    # LIMPIEZA Y EXPORTACIÓN
    # Ajusta los nombres aquí según cómo quieras que se vean en el Excel final
    df_resultado = final[final['DESCRIPCION'] != 0].sort_values(by='COBERTURA_MESES')
    
    print("--- ¡Reporte generado con éxito! ---")
    print(df_resultado.head(10))
    df_resultado.to_excel("Reporte_Stock_Final.xlsx", index=False)

    db_brogas.close()
    db_brogasml.close()

# --- 2. FRONTEND CON COLORES REESTABLECIDOS ---
def run_frontend(df_final, dict_consultas):
    st.set_page_config(page_title="Monitor de Stock Brogas", layout="wide")
    st.title("📊 Panel de Control de Inventario")

    tab1, tab2, tab3 = st.tabs(["🚀 Informe Final", "📦 Stock y Disponibilidad", "🛠️ Producción (OP)"])
    
    cod_excel = df_final['CODIGOPARTICULAR'].unique()

    with tab1:
        st.subheader("Informe Consolidado (Artículos del Excel)")
        
        # Aplicamos colores: Rojo si la cobertura es baja, Verde si es alta
        def color_cobertura(val):
            if val <= 1.0: return 'background-color: #ff0000' # Rojo claro
            if val <= 2.0: return 'background-color: #FFFF00' # Amarillo claro
            return 'background-color: #00ff00' # Verde claro

        st.dataframe(
            df_final.style.applymap(color_cobertura, subset=['COBERTURA_MESES']),
            use_container_width=True, 
            height=600
        )

    with tab2:
        st.subheader("Estado de Stock y Disponibilidad")
        df_stock = dict_consultas['articulos'].copy()
        df_stock = df_stock[df_stock['CODIGOPARTICULAR'].isin(cod_excel)]
        
        # Cálculo de Disponible (asegurando que existan las columnas)
        df_stock['DISPONIBLE'] = df_stock['STOCK'] - df_stock.get('CANTIDAD PEDIDOS', 0)
        st.dataframe(df_stock, use_container_width=True)

    with tab3:
        st.subheader("Órdenes de Producción")
        df_op = dict_consultas['op'].copy()
        df_op = df_op[df_op['CODIGOPARTICULAR'].isin(cod_excel)]
        # Aquí ya aparecerá la columna DESCRIPCION que agregamos en la función SQL
        st.dataframe(df_op, use_container_width=True)

# --- 3. PROCESAMIENTO DE DATOS (REVISIÓN PENDIENTE TOTAL) ---
def procesar_datos():
    conn_brogas = conectar_odbc("BROGAS")
    conn_ml = conectar_odbc("BROGASML")
    
    try:
        # EXTRACCIÓN
        df_proy = get_proyectado_desde_tabla() # Excel
        df_art = get_articulos_3(conn_brogas)  # Fuente oficial de DESCRIPCION y STOCK
        df_ventas = get_cuerpo_comprobantes(conn_brogas) # FA/FB
        df_pedidos = get_pedidos_pendientes(conn_brogas) # La nueva consulta
        df_op = get_op_2(conn_brogas) # Producción
        df_ml = get_cuerpo_comprobantes_ml(conn_ml) # Mercado Libre

        # NORMALIZACIÓN
        for d in [df_proy, df_art, df_ventas, df_pedidos, df_op, df_ml]:
            if d is not None and not d.empty:
                d.columns = d.columns.str.upper()
                if 'CODIGO' in d.columns: d.rename(columns={'CODIGO': 'CODIGOPARTICULAR'}, inplace=True)

        # UNIÓN SOBRE EXCEL
        final = df_proy[['CODIGOPARTICULAR', 'PEDIDO_PROYECTADO']].copy()
        
        # Merge con Artículos para DESCRIPCION (oficial) y STOCK
        final = final.merge(df_art[['CODIGOPARTICULAR', 'DESCRIPCION', 'STOCK']], on='CODIGOPARTICULAR', how='inner')

        # Merges de Pendientes
        final = final.merge(df_ventas, on='CODIGOPARTICULAR', how='left')   # Pendientes FA/FB
        final = final.merge(df_pedidos, on='CODIGOPARTICULAR', how='left')  # Pedidos nuevos
        final = final.merge(df_ml, on='CODIGOPARTICULAR', how='left')       # Mercado Libre
        final = final.merge(df_op[['CODIGOPARTICULAR', 'CANTIDAD EN PROD']], on='CODIGOPARTICULAR', how='left')

        final = final.fillna(0)

        # CÁLCULO PENDIENTE TOTAL (Todas las fuentes)
        # Sumamos: Ventas Pendientes + Pedidos Nuevos + Mercado Libre
        final['PENDIENTE_TOTAL'] = (
            final.get('CANT PENDIENTE', 0) + 
            final.get('PEDIDOS_NUEVOS', 0) + 
            final.get('PENDIENTE_ML', 0)
        )
        
        final['STOCK_NETO'] = final['STOCK'] - final['PENDIENTE_TOTAL']
        
        # Cobertura
        with np.errstate(divide='ignore', invalid='ignore'):
            final['COBERTURA_MESES'] = (final['STOCK_NETO'] / final['PEDIDO_PROYECTADO']) * 3
            final['COBERTURA_MESES'] = final['COBERTURA_MESES'].replace([np.inf, -np.inf], 0).fillna(0)

        # FILTRO FINAL Y ORDEN
        # Usamos 'DESCRIPCION' que viene de df_art (sin tilde y garantizada)
        df_resultado = final.sort_values(by='COBERTURA_MESES')
        
        # Seleccionamos solo las columnas limpias para el reporte
        columnas_visibles = [
            'CODIGOPARTICULAR', 'DESCRIPCION', 'PEDIDO_PROYECTADO', 
            'STOCK', 'PENDIENTE_TOTAL', 'STOCK_NETO', 'COBERTURA_MESES'
        ]
        df_resultado = df_resultado[columnas_visibles]

        return df_resultado, df_art, df_ventas, df_op

    finally:
        if conn_brogas: conn_brogas.close()
        if conn_ml: conn_ml.close()

if __name__ == "__main__":
    # Iniciar la interfaz de Streamlit
    # Streamlit maneja su propio bucle, así que procesamos los datos una vez
    df_res, df_a, df_v, df_o = procesar_datos()
    
    if not df_res.empty:
        consultas = {
            'articulos': df_a,
            'ventas': df_v,
            'op': df_o
        }
        run_frontend(df_res, consultas)
    else:
        st.warning("No se generaron datos. Verifique la conexión y el archivo Excel.")