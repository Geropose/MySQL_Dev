USE [MESSER_CHILE]
GO
/****** Object:  StoredProcedure [dbo].[sp_Cambio_de_Precios_Futuros]    Script Date: 03-02-2023 12:43:25 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- ===============================================================================
-- Author:		EDSA - Geronimo Pose
-- Create date: 03-02-2023 15:54:25
-- Description:	Se actualizan los precios en los clientes, que fueron cargados a futuro
-- ===============================================================================

---- ==========================================================================================
---- Create date: 03/02/2023
---- OBJETIVO: Se obtiene la lista de precios especiales que pertenecen a un cliente 
---- ==========================================================================================

ALTER PROCEDURE [dbo].[sp_obtener_listas_PE]
(
	   @sSucursal varchar(10),
	   @sCliente varchar(20),
	   @sNro_domicilio_cliente varchar(10),
	   @sLista_de_precios varchar(MAX) OUTPUT
)
AS
SET NOCOUNT ON;

BEGIN

	SET NOCOUNT ON;
		DECLARE @swRowidLista_Precios	TIMESTAMP;
		DECLARE @nwLista_de_precio		VARCHAR(15);
		DECLARE @sgMonedaDefault		VARCHAR(10);

		DECLARE @RC INT;
		DECLARE @LogEnabled bit,
			@STEP_TYPE varchar(500),
			@STEP_NAME varchar(500),
			@LOG_TYPE varchar(100),
			@ERR_NUMBER int,
			@ERR_SEVERITY int,
			@ERR_STATE int,
			@SP_ERR_LINE int,
			@MESSAGE varchar(4000),
			@DESTINATION varchar(500),
			@ROWID_ERROR varchar(20),
			@CANT_ERRORES int

		SET @LogEnabled = 'TRUE'
		SET @STEP_TYPE = 'Stored Procedure'
		SET @STEP_NAME = '[sp_Cambio_de_Precios_Futuros]'
		SET @LOG_TYPE = 'INFO'
		SET @ERR_NUMBER = 0
		SET @ERR_SEVERITY = 0
		SET @ERR_STATE = 0
		SET @SP_ERR_LINE = 0
		SET @MESSAGE = NULL
		SET @DESTINATION = 'Cambio de precios futuros'
		SET @ROWID_ERROR = ''
		SET @CANT_ERRORES = 0

	BEGIN
		SELECT
			@swRowidLista_Precios = listas_de_precios.rowid,
			@sLista_de_precios = listas_de_precios.lista_de_precio
		FROM
			listas_de_precios,
			rel_lista_precios_cliente
		WHERE
			listas_de_precios.sucursal = rel_lista_precios_cliente.sucursal AND
			listas_de_precios.lista_de_precio = rel_lista_precios_cliente.lista_de_precio AND
			listas_de_precios.es_lista_precios_especiales = 'S' AND
			rel_lista_precios_cliente.cliente = @sCliente AND
			rel_lista_precios_cliente.nro_domicilio_cliente = @sNro_domicilio_cliente AND
			rel_lista_precios_cliente.sucursal = @sSucursal

			-- PREGUNTAR POR PRIORIDAD DE BUSQUEDA, QUE LISTA DE PRECIO DEBERIA ELEGIR?
			-- YA QUE UN CLIENTE PUEDE TENER DOS LISTAS DE PRECIO
		PRINT('LA LISTA DE PRECIOS TIENE LO SIGUIENTE DENTRO DE PE:' + @sLista_de_precios + ' ' )

		IF (@sLista_de_precios IS NULL) or (@sLista_de_precios = '')-- Ingreso al IF si la lista es null o es igual a vacio
			BEGIN
				BEGIN TRY
					BEGIN TRANSACTION
						PRINT('INGRESO A CREAR UNA NUEVA LISTA')
						--------------------------- UPDATE ---------------------------
						UPDATE
							contadores
						SET
							contadores.contador = contadores.contador + 1
						WHERE
							(contadores.nombre_contador =  'PRECIO_ESP')  AND (contadores.sucursal = @sSucursal)
						--------------------------- SELECT CONTADOR ---------------------------
						SELECT
							@nwLista_de_precio = 'PE_'+ CONVERT(varchar(12),contadores.contador)
						FROM
							contadores
						WHERE
							(contadores.nombre_contador =  'PRECIO_ESP'  AND contadores.sucursal = @sSucursal)
						--------------------------- INSERT INTO LISTA PRECIOS ---------------------------
						INSERT INTO listas_de_precios (
											listas_de_precios.sucursal,
											listas_de_precios.lista_de_precio,
											listas_de_precios.moneda,
											listas_de_precios.descripcion_lista,
											listas_de_precios.vigencia_desde,
											listas_de_precios.vigencia_hasta,
											listas_de_precios.es_lista_precios_especiales
											)
						VALUES (
									@sSucursal,
									@sLista_de_precios,
									@sgMonedaDefault,
									'Precios especiales de ' + @sCliente + '-' +  @sNro_domicilio_cliente,
									GETDATE( ),
									'1-1-2100',
									'S' )
						--------------------------- INSERT INTO LISTA PRECIOS CLIENTES ---------------------------
						PRINT('HAGO EL INSERT INTO RELACION LISTA DE PRECIOS DE CLIENTES')
						INSERT INTO rel_lista_precios_cliente( 
											rel_lista_precios_cliente.sucursal, 
											rel_lista_precios_cliente.lista_de_precio, 
											rel_lista_precios_cliente.cliente, 
											rel_lista_precios_cliente.nro_domicilio_cliente, 
											rel_lista_precios_cliente.prioridad_busqueda, 
											rel_lista_precios_cliente.desc_lista_precios 
											)
						VALUES ( 
									@sSucursal, 
									@sLista_de_precios, 
									@sCliente, 
									@sNro_domicilio_cliente, 
									0,
									0 
									)
					COMMIT TRANSACTION
				END TRY
					BEGIN CATCH
						SET @STEP_TYPE = 'Obtener Listas PE'
						SET @STEP_NAME = '[sp_Cambio_de_Precios_Futuros]' 
						SET @LOG_TYPE = 'ERROR' 
						SET @ERR_NUMBER = ERROR_NUMBER();
						SET @ERR_SEVERITY = ERROR_SEVERITY();
						SET @ERR_STATE = ERROR_STATE();
						SET @SP_ERR_LINE = ERROR_LINE();
						SET @MESSAGE = 'ERROR EN LAS TRANSACCIONES INSERT/UPDATE de la lista de precio: ' + @sLista_de_precios + '. Error:' +  ERROR_MESSAGE()
						SET @DESTINATION = 'SP'

						EXECUTE [dbo].[sp_Cambio_Precios_Logs]
									@STEP_TYPE,
									@STEP_NAME,
									@LOG_TYPE,
									@ERR_NUMBER,
									@ERR_SEVERITY,
									@ERR_STATE,
									@SP_ERR_LINE,
									@MESSAGE,
									@DESTINATION
						
						SET @CANT_ERRORES =  @CANT_ERRORES + 1
					END CATCH
			END
	END
	BEGIN
			IF (@CANT_ERRORES = 0)
				BEGIN
					SET @LOG_TYPE = 'OK' 
					SET @MESSAGE = 'El proceso finalizo sin errores'
				END
			ELSE
				BEGIN
					SET @LOG_TYPE = 'ERROR. Cantidad de errores: ' + CONVERT(VARCHAR(10),@CANT_ERRORES )
					SET @MESSAGE = 'El proceso finalizo con errores'   
				END
			SET @STEP_TYPE='[sp_Cambio_de_Precios_Futuros]'
			SET @ERR_NUMBER = 0 
			SET @ERR_SEVERITY = 0 
			SET @ERR_STATE = ''
			SET @SP_ERR_LINE = '' 
			SET @DESTINATION = 'SP'
  
			EXECUTE [dbo].[sp_Cambio_Precios_Logs]
						@STEP_TYPE,
						@STEP_NAME,
						@LOG_TYPE,
						@ERR_NUMBER,
						@ERR_SEVERITY,
						@ERR_STATE,
						@SP_ERR_LINE,
						@MESSAGE,
						@DESTINATION		
	END
END