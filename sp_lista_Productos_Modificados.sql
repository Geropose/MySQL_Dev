USE [MESSER_CHILE]
GO
/****** Object:  StoredProcedure [dbo].[sp_lista_Productos_Modificados]    Script Date: 03-02-2023 12:43:25 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- ===============================================================================
-- Author:		EDSA - Geronimo Pose
-- Create date: 03-02-2023 15:54:25
-- Description:	Se actualizan los precios a futuro de los clientes
-- ===============================================================================

---- ==========================================================================================
---- Create date: 03/02/2023
---- OBJETIVO: Se obtiene la lista de productos modificados que es utilizada en el SP_cambio_de_precios
---- 
---- ==========================================================================================

ALTER PROCEDURE [dbo].[sp_lista_Productos_Modificados]
( 
	   @sSucursal					VARCHAR(10),
	   @swListaProductosModificados VARCHAR(MAX),
	   @sLista_de_precios			VARCHAR(MAX)
)  
AS
SET NOCOUNT ON;

 BEGIN
	SET NOCOUNT ON;

		--------------------------- BLOQUE DE DECLARE ---------------------------
		DECLARE 
			@cantNiveles					INT = 0,
			@nPrecio_producto_base			DECIMAL(25,12),
			@cngwDecimalesPrecios			VARCHAR(10),
			@ngwDecimalesPrecios			DECIMAL(10,2),
			@sProducto						VARCHAR(10), 
			@sEnvase						VARCHAR(10), 
			@sLista_de_precios_base			VARCHAR(MAX),
			@nPorcentaje_de_la_lista_base	DECIMAL(7,4),
			@test VARCHAR(MAX) = '',
			@testprecio DECIMAL(5) = 0,

			@RC								INT,
			@LogEnabled						BIT, 
			@STEP_TYPE						VARCHAR(500),
			@STEP_NAME						VARCHAR(500),
			@LOG_TYPE						VARCHAR(100),
			@ERR_NUMBER						INT,
			@ERR_SEVERITY					INT,
			@ERR_STATE						INT,
			@SP_ERR_LINE					INT,
			@MESSAGE						VARCHAR(4000),
			@DESTINATION					VARCHAR(500),
			@ROWID_ERROR					VARCHAR(20),
			@CANT_ERRORES					INT

		--------------------------- BLOQUE DE SET ---------------------------
		-- Se setean los valores de las variables cargadas en la Tabla Cambio_Precios_Logs
		SET @LogEnabled = 'TRUE'
		SET @STEP_TYPE = 'Stored Procedure'
		SET @STEP_NAME = '[sp_lista_Productos_Modificados]' 
		SET @LOG_TYPE = 'INFO' 
		SET @ERR_NUMBER = 0
		SET @ERR_SEVERITY = 0
		SET @ERR_STATE = 0
		SET @SP_ERR_LINE = 0
		SET @MESSAGE = NULL
		SET @DESTINATION = 'Lista de productos modificados'
		SET @ROWID_ERROR = ''
		SET @CANT_ERRORES = 0


		PRINT ('CANTIDAD DE TRANSACCIONES DENTRO DE PRODUCTOS MODIFICADOS:' +CONVERT(VARCHAR(12), CONVERT (INT,@@TRANCOUNT )) +'')
		PRINT('LA SUCURSAL CON LA QUE INGRESO ES:' +@sSucursal+ '')
		PRINT('LA LISTA DE PRODUCTOS MODIFICADOS TIENE LO SIGUIENTE:' + @swListaProductosModificados + '' )
		BEGIN
			BEGIN TRY
				BEGIN TRANSACTION -- TRANCOUNT CAMBIA A 2
						----------------------------------------------------------------------------------------------------------------			
						IF EXISTS ( SELECT precio_producto 
									FROM precio_producto
									WHERE	precio_producto.sucursal = @sSucursal AND
											precio_producto.valor_o_porcentaje_lista = 'P' AND
											precio_producto.producto IN (@swListaProductosModificados) AND
											precio_producto.lista_de_precio = @sLista_de_precios)
											BEGIN
												PRINT('VOY A EJECUTAR EL UPDATE Y SETEAR PRECIO PROD EN NULL')
												UPDATE	precio_producto
												SET	precio_producto.precio_producto = NULL
												WHERE precio_producto.sucursal = @sSucursal AND
													precio_producto.lista_de_precio = @sLista_de_precios AND
													precio_producto.valor_o_porcentaje_lista = 'P' AND
													precio_producto.producto IN (@swListaProductosModificados)
												PRINT('VOY A SALIR DEL UPDATE')
											END
						SET @test = (SELECT precio_producto.precio_producto FROM precio_producto WHERE (precio_producto.precio_producto IS NULL) AND (precio_producto.sucursal = @sSucursal) AND (precio_producto.valor_o_porcentaje_lista = 'P') AND precio_producto.producto IN ('01000'))
						PRINT(ISNULL(@test,'NULL'))			
						-----------------------------------------------------------------------------------------------------------------
						DECLARE cniveles CURSOR GLOBAL
						FOR  SELECT 
								precio_producto.sucursal, 
								precio_producto.lista_de_precio, 
								precio_producto.producto, 
								precio_producto.lista_de_precios_base, 
								precio_producto.porcentaje_de_la_lista_base 
							FROM 
								precio_producto
							WHERE
								(precio_producto.precio_producto IS NULL) AND 
								(precio_producto.sucursal = @sSucursal) AND 
								(precio_producto.valor_o_porcentaje_lista = 'P') AND 
								precio_producto.producto IN (@swListaProductosModificados) --DECLARO LOS DATOS QUE VOY A UTILIZAR EN EL CURSOR
						OPEN cniveles
						FETCH cniveles INTO   @sSucursal, @sLista_de_precios, @sProducto, @sLista_de_precios_base, @nPorcentaje_de_la_lista_base
						PRINT('ENTRO AL BUCLE DE CANTIDAD DE SUBNIVELES')
						WHILE (@cantNiveles < 3) AND (@@fetch_status<>-1) -- Si le pongo un AND ambas condiciones deben cumplirse por ende como aun no declare nada en el cursor, el fetch status no es 0, sin embargo
																		-- si le pongo un OR, vuelve a realizar el ciclo una vez mas, ya que no es menor y sin embargo no deberia de encontrar otro valor
							BEGIN
								PRINT('EN EL WHILE ANTES DEL CURSOR')
								PRINT (' suc'+@sSucursal+' prod' +@swListaProductosModificados+'')
								PRINT('Lista de precios BASE: ' +@sLista_de_precios_base+'')
								PRINT('SETEO LOS DECIMALES')
								SET @cngwDecimalesPrecios = (SELECT 
																Parametros_generales.Valor
															FROM
																Parametros_generales
															WHERE
																(Parametros_generales.sucursal = @sSucursal) AND
																(Parametros_generales.Variable = 'ngwDecimalesPrecios'))
								PRINT('CONVIERTO LOS DECIMALES')
								SET @ngwDecimalesPrecios = CONVERT(INT,@cngwDecimalesPrecios)										
								PRINT('DECIMALES: '+ CONVERT(VARCHAR(5),@ngwDecimalesPrecios) + ' ')
								SET @nPrecio_producto_base = (SELECT 
																	precio_producto.precio_producto
																FROM 
																	precio_producto
																WHERE 
																	(precio_producto.sucursal = @sSucursal) AND 
																	(precio_producto.lista_de_precio = @sLista_de_precios_base) AND 
																	(precio_producto.producto = @sProducto))
								print('PUDE SETEAR MI VALOR')
								PRINT('PRODUCTO BASE:' + CONVERT(VARCHAR(MAX),@nPrecio_producto_base)+ ' ')
								PRINT('VOY A INGRESAR AL UPDATE PRODUCTO BASE CON LA SUCURSAL: '+@sSucursal+'')
								PRINT('VOY A INGRESAR AL UPDATE PRODUCTO BASE CON EL PRODUCTO: '+@sProducto+'')
								PRINT('VOY A INGRESAR AL UPDATE PRODUCTO BASE CON LA LISTA DE PRECIOS: '+@sLista_de_precios+'')
								PRINT('REDONDEO:' +CONVERT(VARCHAR(MAX),ROUND((@nPrecio_producto_base * ( 1 + @nPorcentaje_de_la_lista_base / 100 )),  @ngwDecimalesPrecios ))+ '')
								If (@nPrecio_producto_base IS NOT NULL)
									BEGIN
										PRINT('VOY A EJECUTAR EL UPDATE DEL REDONDEO')
										UPDATE 
											precio_producto
										SET
											precio_producto.precio_producto = ROUND((@nPrecio_producto_base * ( 1 + @nPorcentaje_de_la_lista_base / 100 )),  @ngwDecimalesPrecios )
										WHERE 
											(precio_producto.sucursal = @sSucursal) AND 
											(precio_producto.lista_de_precio = @sLista_de_precios) AND  
											(precio_producto.producto = @sProducto)
									END
								PRINT('SALI DEL UPDATE DE PRODUCTO BASE')
								PRINT('PUDE HACER EL FETCH')
								SET @cantNiveles = @cantNiveles + 1;
								PRINT('CANT DE NIVEL:' +CONVERT(VARCHAR(MAX),@cantNiveles)+ '')
								FETCH cniveles INTO   @sSucursal, @sLista_de_precios, @sProducto, @sLista_de_precios_base, @nPorcentaje_de_la_lista_base
							END
				COMMIT TRANSACTION
			END TRY
				BEGIN CATCH
					SET @STEP_TYPE = 'Lista de productos modificados'
					SET @STEP_NAME = '[sp_lista_Productos_Modificados]' 
					SET @LOG_TYPE = 'ERROR' 
					SET @ERR_NUMBER = ERROR_NUMBER();
					SET @ERR_SEVERITY = ERROR_SEVERITY();
					SET @ERR_STATE = ERROR_STATE();
					SET @SP_ERR_LINE = ERROR_LINE();
					SET @MESSAGE = 'ERROR EN LAS TRANSACCIONES INSERT/UPDATE DE LA LISTA DE PRODUCTOS MODIFICADOS: ' + @sLista_de_precios + '. Error:' +  ERROR_MESSAGE()
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
			CLOSE cniveles
			DEALLOCATE cniveles
		END

	BEGIN

		IF (@CANT_ERRORES = 0)
			BEGIN
				PRINT('NO HUBO ERRORES EN LA LISTA DE PRODUCTOS')
				 SET @LOG_TYPE = 'OK' 
				 SET @MESSAGE = 'El proceso finalizo sin errores'
			END
		ELSE
			BEGIN
			  PRINT('HUBO AL MENOS UN ERROR EN LA LISTA DE PRODUCTOS MODIFICADOS')
			  SET @LOG_TYPE = 'ERROR. Cantidad de errores: ' + CONVERT(VARCHAR(10),@CANT_ERRORES )
			  SET @MESSAGE = 'El proceso finalizo con errores'   
			END
	  SET @STEP_TYPE='[sp_lista_Productos_Modificados]'
	  SET @ERR_NUMBER = 0 ; 
	  SET @ERR_SEVERITY = 0 ; 
	  SET @ERR_STATE = ''; 
	  SET @SP_ERR_LINE = '' ; 
	  SET @DESTINATION = 'SP';
  
		
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