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
---- OBJETIVO: Utilizando los datos indicados desde workflow, se actualizan los precios
---- de un producto o producto_envase pertenecientes a un cliente.
---- ==========================================================================================

ALTER PROCEDURE [dbo].[sp_Cambio_de_Precios_Futuros]
AS
SET NOCOUNT ON;
BEGIN
	
	--------------------------- BLOQUE DE DECLARE ---------------------------
	-- Se declaran las variables utilizadas por este sp y por los invocados dentro del mismo
	SET NOCOUNT ON;
		DECLARE @RC INT,
				@LogEnabled BIT, 
				@INTERFACE_NAME VARCHAR(500) = '', 
				@STEP_TYPE VARCHAR(500) = '',
				@STEP_NAME VARCHAR(500) = '',         
				@LOG_TYPE VARCHAR(100) = '',
				@ERR_NUMBER INT,               
				@ERR_SEVERITY INT,
				@ERR_STATE INT,                
				@SP_ERR_LINE INT,
				@MESSAGE VARCHAR(4000) = '',
				@DESTINATION VARCHAR(500) = '',
				@CANT_ERRORES INT,
				@ROWID_ERROR VARCHAR(20) = '',
				@EJECUTAR_INTERFACE CHAR(1),
				@tipo_documento_JDE VARCHAR(2) = '',
    			@nNumero_de_cambio VARCHAR(10) = '', 
				@sSucursal VARCHAR(10) = '',
				@sLista_de_precios VARCHAR(MAX) = '',
				@sLista_de_preciosCopia VARCHAR(MAX) = '',
				@sCliente VARCHAR(20), 
				@sNro_domicilio_cliente VARCHAR(10) = '', 
				@dVigencia_desde DATETIME,
				@dVigencia_hasta DATETIME,
				@sProducto VARCHAR(10) = '',
				@sEnvase VARCHAR(10) = '',
				@sMoneda VARCHAR(10) = '', 
				@nPrecio DECIMAL(13),
				@sCodigo_irp VARCHAR(10) = '', 
				@sLista_de_precios_base VARCHAR(MAX) = '',
				@sValor_o_Porcentaje VARCHAR(1),
				@nPorcentaje DECIMAL(5),
				@nDescuento_maximo DECIMAL(5),
				@nDescuento_cantidad DECIMAL(5),
				@nCantidad_para_descuento DECIMAL(13),
				@sOperador VARCHAR(10) = '',
				@sRowid TIMESTAMP,
				@listaProducto VARCHAR(MAX) = '',
				@swListaProductosModificados VARCHAR(MAX) = '',
				@swListaProductosEnvModificados VARCHAR (MAX) = '',
				@sLista_Direccion VARCHAR(MAX), 
				@sErrorEjecucion BIT = 0,
				@BODYEMAIL VARCHAR(MAX)

		--------------------------- BLOQUE DE SET ---------------------------
		-- Se setean los valores de las variables cargadas en la Tabla Cambio_Precios_Logs
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
		SET @CANT_ERRORES = 0
		SET @ROWID_ERROR = ''

	BEGIN
		DECLARE cinsert CURSOR GLOBAL
		
		--------------------------- SELECT CURSOR ---------------------------	
		-- Este cursor se encarga de obtener los datos de los cambios de precios no procesados y que sean menor a una fecha actual
		FOR  SELECT 
				cambios_de_precios.numero_de_cambio,
				cambios_de_precios.sucursal, 
				cambios_de_precios.lista_de_precio, 
				cambios_de_precios.cliente, 
				cambios_de_precios.nro_domicilio_cliente, 
				cambios_de_precios.desde, 
				cambios_de_precios.hasta, 
				cambios_de_precios.producto, 
				cambios_de_precios.envase, 
				cambios_de_precios.moneda, 
				cambios_de_precios.precio,
				cambios_de_precios.codigo_irp, 
				cambios_de_precios.lista_de_precios_base,
				cambios_de_precios.valor_o_porcentaje_lista,
				cambios_de_precios.porcentaje_de_la_lista_base,
				cambios_de_precios.descuento_maximo,
				cambios_de_precios.descuento_cantidad,
				cambios_de_precios.cantidad_para_descuento,
				cambios_de_precios.operador,
				cambios_de_precios.rowid		
			FROM
				cambios_de_precios
			WHERE
				cambios_de_precios.desde <= CONVERT (varchar(10), GETDATE(), 120) AND
				cambios_de_precios.procesado_sn = 'N'
						
			OPEN cinsert
			FETCH cinsert INTO  @nNumero_de_cambio,@sSucursal,@sLista_de_precios, @sCliente,@sNro_domicilio_cliente, 
				                @dVigencia_desde, @dVigencia_hasta,@sProducto,@sEnvase,
				                @sMoneda,@nPrecio,@sCodigo_irp,@sLista_de_precios_base,
				                @sValor_o_Porcentaje,@nPorcentaje,@nDescuento_maximo,@nDescuento_cantidad,
				                @nCantidad_para_descuento,@sOperador, @sRowid
			-- Comienza el ciclo del cursor CINSERT
			WHILE(@@fetch_status=0)
				BEGIN
					PRINT('INGRESO AL NUMERO DE CAMBIO: ' +@nNumero_de_cambio+' ')
	                SET @ROWID_ERROR  = @sRowid
					PRINT ('CANTIDAD DE TRANSACCIONES AL COMIENZO DE EJECUCION:' +CONVERT(VARCHAR(12), CONVERT (INT,@@TRANCOUNT )) +'')
	                IF (@sLista_de_precios IS NULL)
						BEGIN
							-- CORRECCIONES A REALIZAR -> BUSQUE EN PRODUCCION SI HABIA ALGUN CLIENTE SIN LISTA ESPECIAL Y NO HAY NADIE,
							-- POR ENDE SE SUPONE QUE NO DEBERIA DE TENER QUE CREAR UNA LISTA ESPECIAL, SIN EMBARGO.
							-- LAS TRANSACCIONES ESTAN MAL PUESTAS Y SI HAY UN ERROR EN LA CREACION, EL ROLLBACK ME VA A ROMPER TODO MI PROCESO
							-- YA QUE SALDRIA CON UN NUMERO DISTINTO DE TRANSACCIONES.

							-- La lista de precios siempre va a ser null ya que viene precargada desde workflow con valor NULL
							-- por lo tanto se buscara las relaciones de la misma para poder hallar una referencia
							-- En caso de que la lista de precios venga con valor NULL, entonces se ejecuta el SP que obtiene
							-- la lista de Precios Especiales de un cliente, dicho sp guarda la lista en la variable @sLista_de_precios
							EXECUTE [dbo].[sp_obtener_listas_PE]
												 @sSucursal
												,@sCliente
												,@sNro_domicilio_cliente
												,@sLista_de_precios = @sLista_de_preciosCopia OUTPUT
						END
					SET @sLista_de_precios = @sLista_de_preciosCopia
					PRINT('LA LISTA DE PRECIOS TIENE LO SIGUIENTE:' + @sLista_de_precios + ' SALIENDO DE LA RAMA PE' )
					PRINT(ISNULL(@sEnvase,'ENVASE NULL'))
					PRINT ('CANTIDAD DE TRANSACCIONES ANTES DE EJECUCION:' +CONVERT(VARCHAR(12), CONVERT (INT,@@TRANCOUNT )) +'')
					IF (@sEnvase IS NULL)
						BEGIN
							BEGIN TRY
								BEGIN TRANSACTION -- 1
									PRINT ('CANTIDAD DE TRANSACCIONES EN EL PRIMER BEGIN TRAN:' +CONVERT(VARCHAR(12), CONVERT (INT,@@TRANCOUNT )) +'')
									-- En el caso de que el cambio de precio pertenezca a un Producto, entonces ingreso por esta rama
									-- ya que el valor correspondiente al campo Envase sera NULL
									PRINT('LA LISTA ES UN VALOR DE TIPO:' +@sValor_o_Porcentaje+ '')
									--------------------------- EXIST Y DELETE PRECIO PRODUCTO ---------------------------
									-- Se pregunta si existe el precio del producto, en caso de exista
									-- entonces se borra
									IF EXISTS (
												SELECT 
													precio_producto.precio_producto
												FROM 
													precio_producto
												WHERE
													precio_producto.sucursal = @sSucursal AND
													precio_producto.lista_de_precio = @sLista_de_precios AND
													precio_producto.producto = @sProducto)
													BEGIN
														DELETE FROM precio_producto
														WHERE 
															precio_producto.sucursal = @sSucursal AND 
															precio_producto.lista_de_precio = @sLista_de_precios AND 
															precio_producto.producto = @sProducto										
													END
									--------------------------- INSERT INTO AUDITORIA ---------------------------
									-- Se inserta una pista de auditoria
									INSERT INTO	auditoria (
													auditoria.fecha_hora_operacion, 
													auditoria.sucursal, 
													auditoria.operador, 
													auditoria.riesgo, 
													auditoria.operacion_auditada, 
													auditoria.detalles_operacion, 
													auditoria.monto_operacion, 
													auditoria.dato_adicional_1, 
													auditoria.dato_adicional_2)
									VALUES (
											GETDATE(),
											@sSucursal,
											@sOperador,
											'ALTO',
											'DPREPRO',
											'Ingreso de precio del producto :'+ @sProducto + ' Precio: ' + CONVERT(VARCHAR(12), CONVERT (INT,@nPrecio )) + ' Operador: ' + @sOperador + ' ',
											CONVERT(VARCHAR(12), CONVERT (INT,@nPrecio)),
											NULL,
											NULL)
									--------------------------- INSERT INTO PRECIO PRODUCTO ---------------------------
									-- Como en el paso de arriba se borro el precio, ahora se procede a insertar el nuevo
									INSERT INTO precio_producto (precio_producto.sucursal,
																precio_producto.lista_de_precio,
																precio_producto.producto,
																precio_producto.vigencia_desde,
																precio_producto.vigencia_hasta,
																precio_producto.valor_o_porcentaje_lista,
																precio_producto.precio_producto,
																precio_producto.moneda,
																precio_producto.lista_de_precios_base,
																precio_producto.porcentaje_de_la_lista_base,
																precio_producto.descuento_maximo,
																precio_producto.descuento_cantidad,
																precio_producto.cantidad_para_descuento,
																precio_producto.Codigo_IRP,
																precio_producto.fecha_modif_alta_reg)
									VALUES(
											@sSucursal,
											@sLista_de_precios,
											@sProducto,
											@dVigencia_desde,
											@dVigencia_hasta,
											ISNULL(@sValor_o_Porcentaje,'V'),
											@nPrecio,
											@sMoneda,
											@sLista_de_precios_base,
											@nPorcentaje,
											ISNULL(@nDescuento_maximo, 0),
											ISNULL(@nDescuento_cantidad, 0),
											ISNULL(@nCantidad_para_descuento, 0),
											ISNULL(@sCodigo_irp, 'SIN_AJUSTE'),
											GETDATE())				

									--------------------------- INSERT INTO AUDITORIA ---------------------------
									-- Se inserta una pista de auditoria
									INSERT INTO	auditoria (
															auditoria.fecha_hora_operacion, 
															auditoria.sucursal, 
															auditoria.operador, 
															auditoria.riesgo, 
															auditoria.operacion_auditada, 
															auditoria.detalles_operacion, 
															auditoria.monto_operacion, 
															auditoria.dato_adicional_1, 
															auditoria.dato_adicional_2)
												VALUES (
														GETDATE(), 
														@sSucursal, 
														@sOperador, 
														'ALTO', 
														'DPREPRO',
														'Ingreso de precio del producto :'+ @sProducto + ' Precio: ' + CONVERT(VARCHAR(12), CONVERT (INT,@nPrecio )) + ' Operador: ' + @sOperador + ' ',
														CONVERT(VARCHAR(12), CONVERT (INT,@nPrecio )), 
														NULL, 
														NULL)
									
									--------------------------- UPDATE CAMBIO DE PRECIOS ---------------------------									
									-- Se actualiza la tabla Cambio de precio, es decir, dicha solicitud de cambio
									-- se pasa a procesado
									IF EXISTS (	SELECT 1 
												FROM Cambios_de_precios
												WHERE cambios_de_precios.rowid = @sRowid AND
													  cambios_de_precios.valor_o_porcentaje_lista = 'V')
														BEGIN
															UPDATE cambios_de_precios
															SET 
																cambios_de_precios.procesado_sn = 'S',
																cambios_de_precios.fecha_proceso = CONVERT (varchar(10), GETDATE(), 120),
																cambios_de_precios.fecha_modif_alta_reg = GETDATE()
															WHERE 
																cambios_de_precios.rowid = @sRowid AND
																cambios_de_precios.valor_o_porcentaje_lista = 'V'
														END
									SET @sErrorEjecucion = 1
									PRINT('ES VALOR O PORCENTAJE:' +@sValor_o_Porcentaje+ '')
									--------------------------- RAMA DE PORCENTAJE ---------------------------
									IF (@sValor_o_Porcentaje = 'P')
									BEGIN
										PRINT('INGRESO A MI RAMA PRECIO PORCENTAJE - CAMBIO DE PRECIO DE UN PRODUCTO')
										-- Si se da el caso de que dicho cambio de precio sea de Porcentaje, entonces
										-- procedemos a ejecutar las query's de abajo
										DECLARE clistaprecioprod CURSOR GLOBAL
										--------------------------- SELECT CURSOR LISTA PRECIO PRODUCTO ---------------------------
										-- Este cursor concatena todos los productos, armando asi una lista de todos 
										-- los que se van a modificar
										FOR SELECT 
												cambios_de_precios.producto
											FROM 
												cambios_de_precios
											WHERE 
												cambios_de_precios.desde <= CONVERT (varchar(10), GETDATE(), 120) AND
												cambios_de_precios.procesado_sn = 'N' AND
												cambios_de_precios.cliente = @sCliente AND
												cambios_de_precios.valor_o_porcentaje_lista = 'P' AND
												Cambios_de_precios.nro_domicilio_cliente = @sNro_domicilio_cliente
												-- me corresponde seleccionar los productos de mi lista de PE?
												OPEN clistaprecioprod
												FETCH clistaprecioprod INTO @sProducto
												-- Comienza el ciclo del cursor clistaprecioprod	
												WHILE(@@fetch_status=0)
													BEGIN
														PRINT('Estos productos pertenecen al cliente:' + @sCliente + '')
														SET @swListaProductosModificados = @swListaProductosModificados + '' +  @sProducto + '' + ',' 
														FETCH clistaprecioprod INTO @sProducto
													END
												CLOSE clistaprecioprod
												DEALLOCATE clistaprecioprod
										SET @swListaProductosModificados = (SELECT LEFT(@swListaProductosModificados, LEN(@swListaProductosModificados)-1))
										-- Se realiza la modificacion de los cambios de precios de los productos
										-- pertenecientes a porcentajes
										PRINT('LA LISTA PRODUCTOS TIENE EL PRODUCTO:' + @swListaProductosModificados + '')
										-- SI YO LE ENVIO MI VARIABLE LISTA DE PRECIOS, ME VA A ENVIAR LA LISTA DE PRECIO DEL CAMBIO DE PRECIO, NO LA LISTA BASE.
										-- A MI ME INTERESA MODIFICAR EN LA LISTA PRODUCTOS MODIFICADOS, LA LISTA PE O LA LISTA DIRECT??
										EXECUTE [dbo].[sp_lista_Productos_Modificados] -- TRAN COUNT = 1 INGRESA
													 @sSucursal,
													 @swListaProductosModificados,
													 @sLista_de_precios
										SET @sErrorEjecucion = 1
										PRINT('SALIENDO DE LA RAMA PRECIO PRODUCTO PORCENTAJE')
										IF EXISTS (	SELECT 1 
													FROM Cambios_de_precios
													WHERE cambios_de_precios.rowid = @sRowid AND
															cambios_de_precios.valor_o_porcentaje_lista = 'P')
															BEGIN
																UPDATE cambios_de_precios
																SET 
																	cambios_de_precios.procesado_sn = 'S',
																	cambios_de_precios.fecha_proceso = CONVERT (varchar(10), GETDATE(), 120),
																	cambios_de_precios.fecha_modif_alta_reg = GETDATE()
																WHERE 
																	cambios_de_precios.rowid = @sRowid AND
																	cambios_de_precios.valor_o_porcentaje_lista = 'P'
															END
									END
								COMMIT TRANSACTION
								PRINT('YA HICE EL COMMIT Y SIGO CON EL PROXIMO CAMBIO DE PRECIO - HAGO FETCH -')
								FETCH cinsert INTO  @nNumero_de_cambio,@sSucursal,@sLista_de_precios, @sCliente,@sNro_domicilio_cliente, 
													@dVigencia_desde, @dVigencia_hasta,@sProducto,@sEnvase,
													@sMoneda,@nPrecio,@sCodigo_irp,@sLista_de_precios_base,
													@sValor_o_Porcentaje,@nPorcentaje,@nDescuento_maximo,@nDescuento_cantidad,
													@nCantidad_para_descuento,@sOperador, @sRowid
							END TRY
								BEGIN CATCH
									ROLLBACK TRANSACTION
										PRINT('PRINT ROLLBACK PRODUCTO')
										SET @STEP_TYPE = 'Obtener Listas PE'
										SET @STEP_NAME = '[sp_Cambio_de_Precios_Futuros]' 
										SET @LOG_TYPE = 'ERROR'
										SET @ERR_NUMBER = ERROR_NUMBER();
										SET @ERR_SEVERITY = ERROR_SEVERITY();
										SET @ERR_STATE = ERROR_STATE();
										SET @SP_ERR_LINE = ERROR_LINE();
										SET @MESSAGE = 'ERROR EN LAS TRANSACCIONES INSERT/UPDATE DEL PRODUCTO: ' + @sProducto + 'LISTA DE PRECIO: '+ @sLista_de_precios + '. Error:' +  ERROR_MESSAGE()
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
									
										SET @sErrorEjecucion = 0
										SET @CANT_ERRORES =  @CANT_ERRORES + 1
								END CATCH
						END
					ELSE IF NOT ((@sProducto IS NULL) AND (@sEnvase IS NULL))
						BEGIN
							BEGIN TRY
								BEGIN TRANSACTION
									PRINT('INGRESE DENTRO DE PRODUCTO-ENVASE')
									PRINT('LA LISTA ES UN VALOR DE TIPO:' +@sValor_o_Porcentaje+ '')
									PRINT('INGRESO A MI RAMA PRECIO VALOR - CAMBIO DE PRECIO DE UN PRODUCTO ENVASE')
									--------------------------- EXIST Y DELETE PRECIO PRODUCTO ENVASE ---------------------------
									-- Se pregunta si existe el precio del producto-envase, en caso de exista
									-- entonces se borra
									IF EXISTS (
												SELECT 
													precio_producto_envase.precio_producto_envase
												FROM
													precio_producto_envase
												WHERE 
													precio_producto_envase.sucursal = @sSucursal AND 
													precio_producto_envase.lista_de_precio = @sLista_de_precios AND
													precio_producto_envase.producto = @sProducto AND
													precio_producto_envase.envase =  @sEnvase)
														BEGIN
															DELETE FROM  precio_producto_envase
															WHERE 
																precio_producto_envase.lista_de_precio = @sLista_de_precios AND 
																precio_producto_envase.sucursal = @sSucursal AND 
																precio_producto_envase.producto = @sProducto AND 
																precio_producto_envase.envase = @sEnvase
														END
														--------------------------- INSERT INTO AUDITORIA ---------------------------
									-- Se inserta una pista de auditoria
									PRINT('INSERTO UNA PISTA EN AUDITORIA')
									INSERT INTO	auditoria (
												auditoria.fecha_hora_operacion, 
												auditoria.sucursal, 
												auditoria.operador, 
												auditoria.riesgo, 
												auditoria.operacion_auditada, 
												auditoria.detalles_operacion, 
												auditoria.monto_operacion, 
												auditoria.dato_adicional_1, 
												auditoria.dato_adicional_2)
									VALUES (
										GETDATE(), 
										@sSucursal, 
										@sOperador, 
										'ALTO', 
										'DPREPRO',
										'Ingreso de precio del producto :'+ @sProducto + ' Precio: ' + CONVERT(VARCHAR(12), CONVERT (INT,@nPrecio )) + ' Operador: ' + @sOperador + ' ',
										CONVERT(VARCHAR(12), CONVERT (INT,@nPrecio)), 
										NULL, 
										NULL)

									--------------------------- INSERT INTO PRECIO PRODUCTO ENVASE ---------------------------
									-- Como en el paso de arriba se borro el precio, ahora se procede a insertar el nuevo
									PRINT('INSERTO EN PRECIO PRODUCTO ENVASE')
									INSERT INTO precio_producto_envase (
													precio_producto_envase.lista_de_precio, 
													precio_producto_envase.sucursal, 
													precio_producto_envase.producto, 
													precio_producto_envase.envase, 
													precio_producto_envase.vigencia_desde, 
													precio_producto_envase.vigencia_hasta, 
													precio_producto_envase.valor_o_porcentaje_lista, 
													precio_producto_envase.precio_producto_envase, 
													precio_producto_envase.moneda, 
													precio_producto_envase.lista_de_precios_base, 
													precio_producto_envase.porcentaje_de_la_lista_base ,
													precio_producto_envase.descuento_maximo, 
													precio_producto_envase.descuento_cantidad, 
													precio_producto_envase.cantidad_para_descuento, 
													precio_producto_envase.codigo_irp, 
													precio_producto_envase.fecha_modif_alta_reg
													)
											VALUES ( 
													@sLista_de_precios, 
													@sSucursal, 
													@sProducto, 
													@sEnvase, 
													@dVigencia_desde, 
													@dVigencia_hasta, 
													ISNULL( @sValor_o_Porcentaje, 'V'),
													@nPrecio, 
													@sMoneda,
													@sLista_de_precios_base,
													@nPorcentaje, 
													ISNULL( @nDescuento_maximo, 0),
													ISNULL( @nDescuento_cantidad, 0),
													ISNULL( @nCantidad_para_descuento, 0),
													ISNULL( @sCodigo_irp, 'SIN_AJUSTE'), 
													GETDATE())

									--------------------------- INSERT INTO AUDITORIA ---------------------------
									-- Se inserta una pista de auditoria
									PRINT('INSERTO UNA PISTA EN AUDITORIA')
									INSERT INTO	auditoria (
															auditoria.fecha_hora_operacion, 
															auditoria.sucursal, 
															auditoria.operador, 
															auditoria.riesgo, 
															auditoria.operacion_auditada, 
															auditoria.detalles_operacion, 
															auditoria.monto_operacion, 
															auditoria.dato_adicional_1, 
															auditoria.dato_adicional_2)
												VALUES (
														GETDATE(), 
														@sSucursal, 
														@sOperador, 
														'ALTO', 
														'DPREPRO',
														'Ingreso de precio del producto :'+ @sProducto + ' Precio: ' + CONVERT(VARCHAR(12), CONVERT (INT,@nPrecio )) + ' Operador: ' + @sOperador + ' ',
														CONVERT(VARCHAR(12), CONVERT (INT,@nPrecio )), 
														NULL, 
														NULL)

									--------------------------- UPDATE CAMBIO DE PRECIOS ---------------------------									
									-- Se actualiza la tabla Cambio de precio, es decir, dicha solicitud de cambio
									-- se pasa a procesado
									PRINT('HICE EL UPDATE CORRESPONDIENTE DE CAMBIO DE PRECIO')
									UPDATE cambios_de_precios
									SET 
										cambios_de_precios.procesado_sn = 'S',
										cambios_de_precios.fecha_proceso = CONVERT (varchar(10), GETDATE(), 120),
										cambios_de_precios.fecha_modif_alta_reg = getDate()
									WHERE 
										cambios_de_precios.rowid = @sRowid
								
									SET @sErrorEjecucion = 1
									IF (@sValor_o_Porcentaje = 'P')
										BEGIN
											PRINT('INGRESO A MI RAMA PRECIO PORCENTAJE - CAMBIO DE PRECIO DE UN PRODUCTO ENVASE')
											-- Si se da el caso de que dicho cambio de precio sea de Porcentaje, entonces
											-- procedemos a ejecutar las query's de abajo
											DECLARE clistaprecioprodenv CURSOR GLOBAL
											--------------------------- SELECT CURSOR LISTA PRECIO PRODUCTO ENVASE ---------------------------
											-- Este cursor concatena todos los productos-envase, armando asi una lista de todos 
											-- los que se van a modificar
											FOR SELECT 
													cambios_de_precios.producto,
													cambios_de_precios.envase
												FROM 
													cambios_de_precios
												WHERE 
													cambios_de_precios.desde <= CONVERT (varchar(10), GETDATE(), 120) AND
													cambios_de_precios.procesado_sn = 'N' AND
													cambios_de_precios.cliente = @sCliente AND
													cambios_de_precios.valor_o_porcentaje_lista = 'P' AND
													Cambios_de_precios.nro_domicilio_cliente = @sNro_domicilio_cliente

												OPEN clistaprecioprodenv
												FETCH clistaprecioprodenv INTO @sProducto, @sEnvase
												-- Comienza el ciclo del cursor clistaprecioprodenv	
												WHILE(@@fetch_status=0)
													BEGIN
														PRINT('ESTOY INSERTANDO PRODUCTOS ENVASES EN LA LISTA A MODIFICAR')
														SET @swListaProductosEnvModificados = @swListaProductosEnvModificados + '''' +  @sProducto + '''' +  @sEnvase + '''' + ',' 
														FETCH clistaprecioprodenv INTO @sProducto
													END
												CLOSE clistaprecioprodenv
												DEALLOCATE clistaprecioprodenv
											SET @swListaProductosEnvModificados = (SELECT LEFT(@swListaProductosEnvModificados, LEN(@swListaProductosEnvModificados)-1))
											-- Se realiza la modificacion de los cambios de precios de los productos-envases
											-- pertenecientes a porcentajes
											PRINT('LA LISTA PRODUCTOS ENVASES TIENE LO SIGUIENTE:' + @swListaProductosEnvModificados + ' ')
											EXECUTE [dbo].[sp_lista_Productos_Envases_Modificados]
														@sSucursal
														,@swListaProductosEnvModificados
														,@sLista_de_precios
											SET @sErrorEjecucion = 1
											PRINT('SALIENDO DE LA RAMA PRECIO PRODUCTO ENVASE PORCENTAJE')
										END
								COMMIT TRANSACTION
								PRINT('YA HICE EL COMMIT Y SIGO CON EL PROXIMO CAMBIO DE PRECIO - HAGO FETCH -')
								FETCH cinsert INTO  @nNumero_de_cambio,@sSucursal,@sLista_de_precios, @sCliente,@sNro_domicilio_cliente, 
													@dVigencia_desde, @dVigencia_hasta,@sProducto,@sEnvase,
													@sMoneda,@nPrecio,@sCodigo_irp,@sLista_de_precios_base,
													@sValor_o_Porcentaje,@nPorcentaje,@nDescuento_maximo,@nDescuento_cantidad,
													@nCantidad_para_descuento,@sOperador, @sRowid
							END TRY
								BEGIN CATCH
									ROLLBACK TRANSACTION
										SET @STEP_TYPE = 'Obtener Listas PE'
										SET @STEP_NAME = '[sp_Cambio_de_Precios_Futuros]' 
										SET @LOG_TYPE = 'ERROR' 
										SET @ERR_NUMBER = ERROR_NUMBER();
										SET @ERR_SEVERITY = ERROR_SEVERITY();
										SET @ERR_STATE = ERROR_STATE();
										SET @SP_ERR_LINE = ERROR_LINE();
										SET @MESSAGE = 'ERROR EN LAS TRANSACCIONES INSERT/UPDATE DEL PRODUCTO: ' + @sProducto + 'ENVASE:' + @sEnvase + 'LISTA DE PRECIO: '+ @sLista_de_precios + '. Error:' +  ERROR_MESSAGE()
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

										SET @sErrorEjecucion = 0
										SET @CANT_ERRORES =  @CANT_ERRORES + 1
								END CATCH
						END
					--EXECUTE [dbo].[sp_HallarDestinatario]
					-- 						@sSucursal
					--						,@sCodigo_de_tarea
					--						,@sLista_Direccion
					--						,@sErrorEjecucion
					--IF (@sLista_Direccion IS NOT NULL)
					-- 	IF (@sErrorEjecucion = 0)
					-- 		BEGIN
					--			SET @BODYEMAIL = 'Hubo un error en la tarea '  + @sCodigo_de_tarea + ' en la sucursal ' + @sSucursal + ' con error: ' + @@ERROR + ' '
					--			EXEC MSDB.DBO.SP_SEND_DBMAIL  @PROFILE_NAME = @sOperador,  @RECIPIENTS = @sLista_Direccion,  @BODY = @BODYEMAIL,  @SUBJECT = '- FRONT OFFICE - Cambio de Precio'; 
					-- 		END
					--	ELSE
					--		BEGIN
					--			SET @BODYEMAIL = 'La tarea '  + @sCodigo_de_tarea + ' de la sucursal ' + @sSucursal + ' se ejecuto Correctamente'
					--			EXEC MSDB.DBO.SP_SEND_DBMAIL  @PROFILE_NAME = @sOperador,  @RECIPIENTS = @sLista_Direccion,  @BODY = @BODYEMAIL,  @SUBJECT = '- FRONT OFFICE - Cambio de Precio'; 
					--		END				
				END
		PRINT('LLEGUE HASTA EL CLOSE DEL CURSOR')
		CLOSE cinsert
		DEALLOCATE cinsert
	END

	BEGIN
		SET @INTERFACE_NAME = '[sp_Cambio_de_Precios_Futuros]'
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
		SET @STEP_TYPE='SP'
		SET @ERR_NUMBER = 0 ; 
		SET @ERR_SEVERITY = 0 ; 
		SET @ERR_STATE = '';
		SET @SP_ERR_LINE = '' ; 
		SET @DESTINATION = 'Cambio de precio futuros';
		
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