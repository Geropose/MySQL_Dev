BEGIN    
 	DECLARE 
		@nNumero_de_cambio varchar(10), 
		@sSucursal varchar(10),
		@sLista_de_precios varchar(10), 
		@sCliente varchar(20), 
		@sNro_domicilio_cliente varchar(10), 
		@dVigencia_desde datetime,
		@dVigencia_hasta datetime,
		@sProducto varchar(10), 
		@sEnvase varchar(10), 
		@sMoneda varchar(10), 
		@nPrecio decimal(25,12),
		@sCodigo_irp varchar(10), 
		@sLista_de_precios_base varchar(10),
		@sValor_o_Porcentaje varchar(1),
		@nPorcentaje decimal(7,4),
		@nDescuento_maximo decimal(7,4),
		@nDescuento_cantidad decimal(7,4),
		@nCantidad_para_descuento decimal(20,4),
		@sOperador varchar(10),
		@sRowid timestamp

	DECLARE cinsert CURSOR GLOBAL
		
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
			cambios_de_precios.desde <= CONVERT (varchar(10), GETDATE(), 120) AND -- VER QUE VARIABLE DEJAR EN VEZ DEL GETDATE
			cambios_de_precios.procesado_sn = 'N'
				
		OPEN cinsert

		FETCH cinsert INTO   @nNumero_de_cambio,@sSucursal,@sLista_de_precios, @sCliente,@sNro_domicilio_cliente, 
		                     @dVigencia_desde, @dVigencia_hasta,@sProducto,@sEnvase,
		                     @sMoneda,@nPrecio,@sCodigo_irp,@sLista_de_precios_base,
		                     @sValor_o_Porcentaje,@nPorcentaje,@nDescuento_maximo,@nDescuento_cantidad,
		                     @nCantidad_para_descuento,@sOperador

		WHILE(@@fetch_status=0)
		BEGIN
                SET @ROWID_ERROR  = @sRowid 
                
                IF (@sLista_de_precios IS NULL) --Todos los que estan en foc sust, son null
					BEGIN 
					-- traigo la lista especial del cliente -SIEMPRE TIENE QUE TENER UNA LISTA DE PRECIO, BASICA O ESPECIAL
						-- Ejecuto mi sp_obtener_listas_PE ??
						--If NOT ObtieneListaPreciosEspeciales(sSucursal,sCliente,sNro_domicilio_cliente,sLista_de_precios ) -- devuelve un booleano true o false y retorna un error
						--Set swError = swError || 'No se obtuvo la lista especial del cliente:' || sCliente ||''                                  
					END

				IF (@sEnvase IS NULL)
					BEGIN
						BEGIN TRY
							BEGIN TRANSACTION
							--! Se va armando las lista de productos para pasar como parametro al dlgPreciosProdDependientes.-
							--Set swListaProductosModificados = swListaProductosModificados || '\'' || sProducto || '\','
								IF EXISTS (
										SELECT precio_producto.precio_producto
										FROM precio_producto
										WHERE
											precio_producto.sucursal = @sSucursal AND
											precio_producto.lista_de_precio = @sLista_de_precios AND
											precio_producto.producto = @sProducto
										)
											BEGIN
												DELETE FROM  precio_producto
												WHERE 
													precio_producto.sucursal = @sSucursal AND 
													precio_producto.lista_de_precio = @sLista_de_precios AND 
													precio_producto.producto = @sProducto										
											END
								INSERT INTO precio_producto (
												precio_producto.sucursal,
												precio_producto.lista_de_precio, 
												precio_producto.producto, 
												precio_producto.vigencia_desde, 
												precio_producto.vigencia_hasta, 
												precio_producto.valor_o_porcentaje_lista, 
												precio_producto.precio_producto,
												precio_producto.moneda, 
												precio_producto.lista_de_precios_base, 
												precio_producto.porcentaje_de_la_lista_base ,
												precio_producto.descuento_maximo, 
												precio_producto.descuento_cantidad, 
												precio_producto.cantidad_para_descuento, 
												precio_producto.codigo_irp,  
												precio_producto.fecha_modif_alta_reg
												)
									VALUES( 
											@sSucursal, 
											@sLista_de_precios, 
											@sProducto, 
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
											GETDATE()
											)
									--		! ! inserto una pista de auditoria por la insercion del nuevo  precio del producto en precio_producto
									--		If NOT GrabarPistaAuditoria( 'ALTO', 'ALTAPPTA',0,sProducto,SalNumberToStrX(nPrecio,12),sLista_de_precios, sOperador, SalNumberToStrX(nNumero_de_cambio,0), sSucursal, '', '', '', '')
									--			Set swError = swError || 'No se pudo grabar auditoria de insercion de precio, producto:'||sProducto|| 'lista:' ||sLista_de_precios||'
								BEGIN
									UPDATE cambios_de_precios
									SET 
										cambios_de_precios.procesado_sn = 'S',
										cambios_de_precios.fecha_proceso = CONVERT (varchar(10), GETDATE(), 120),
										cambios_de_precios.fecha_modif_alta_reg = getDate()
									WHERE 
										cambios_de_precios.rowid = @sRowid
								END	
							COMMIT TRANSACTION
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
								SET @MESSAGE = 'ERROR EN LAS TRANSACCIONES INSERT/UPDATE de la lista de precio: ' + @sLista_de_precios + '. Error:' +  ERROR_MESSAGE()
								SET @DESTINATION = 'SP'

								EXECUTE @RC = [dbo].[sp_Cambio_Precios_Logs]
										   @STEP_TYPE ,@STEP_NAME ,@LOG_TYPE ,@ERR_NUMBER ,@ERR_SEVERITY ,@ERR_STATE ,@SP_ERR_LINE ,@MESSAGE,@DESTINATION
						
								SET @CANT_ERRORES =  @CANT_ERRORES + 1
							END CATCH
					END
				ELSE
					BEGIN
						BEGIN TRY
							BEGIN TRANSACTION
								--		! Se va armando las lista de productos-envases para pasar como parametro al dlgPreciosProdEnvDependientes.-
								--		Set swListaProductosEnvModificados = swListaProductosEnvModificados || '\'' || sProducto || '-' || sEnvase || '\','
								IF EXISTS (
											SELECT 
												precio_producto_envase.precio_producto_envase

											FROM
												precio_producto_envase

											WHERE 
												precio_producto_envase.sucursal = @sSucursal AND 
												precio_producto_envase.lista_de_precio = @sLista_de_precios AND
												precio_producto_envase.producto = @sProducto AND
												precio_producto_envase.envase =  @sEnvase
											)
													BEGIN
														DELETE FROM  precio_producto_envase
														WHERE 
															precio_producto_envase.lista_de_precio = @sLista_de_precios AND 
															precio_producto_envase.sucursal = @sSucursal AND 
															precio_producto_envase.producto = @sProducto AND 
															precio_producto_envase.envase = @sEnvase
													END
								--			If NOT GrabarPistaAuditoria( 'ALTO', 'DPREPROENV', nPrecioAntes,sLista_de_precios, sProducto, sEnvase,SalNumberToStrX(nPrecioAntes,12), '', '', '', '', '', '')
								--				Set swError = swError || 'No se pudo grabar auditoria de borrado de precio, producto:' || sProducto ||' envase:'|| sEnvase|| ' lista:' ||sLista_de_precios||'
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
												GETDATE() 
												)

								--		If NOT SqlPrepareAndExecute( hgwSql, swSqlInsertPrecioProdEnv ) TENGO QUE INSERTAR EL PRECIO PRODUCTO ENVASE
								--			Set swError = swError || 'No se pudo insertar en precio_producto:' || sProducto ||' envase:'|| sEnvase|| ' lista:' ||sLista_de_precios||'
								--					'
								--		! ! inserto una pista de auditoria por la insercion del nuevo  precio del producto-envase en precio_producto_envase
								--		If NOT GrabarPistaAuditoria( 'ALTO', 'ALTAPPETA',0,sProducto, sEnvase, SalNumberToStrX(nPrecio,12),sLista_de_precios, sOperador, SalNumberToStrX(nNumero_de_cambio,0), sSucursal, '', '', '')
								--			Set swError = swError || 'No se pudo grabar auditoria de insercion de precio, producto:'||sProducto||  'envase:' ||sEnvase||  'lista:' ||sLista_de_precios||'

								UPDATE cambios_de_precios

								SET 
									cambios_de_precios.procesado_sn = 'S',
									cambios_de_precios.fecha_proceso = CONVERT (varchar(10), GETDATE(), 120),
									cambios_de_precios.fecha_modif_alta_reg = getDate()

								WHERE 
									cambios_de_precios.rowid = @sRowid
							COMMIT TRANSACTION
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
								SET @MESSAGE = 'ERROR EN LAS TRANSACCIONES INSERT/UPDATE de la lista de precio: ' + @sLista_de_precios + '. Error:' +  ERROR_MESSAGE()
								SET @DESTINATION = 'SP'

								EXECUTE @RC = [dbo].[sp_Cambio_Precios_Logs]
										   @STEP_TYPE ,@STEP_NAME ,@LOG_TYPE ,@ERR_NUMBER ,@ERR_SEVERITY ,@ERR_STATE ,@SP_ERR_LINE ,@MESSAGE,@DESTINATION
						
								SET @CANT_ERRORES =  @CANT_ERRORES + 1
							END CATCH
					END
			END
END




					
				

--While SqlFetchNext( hgwSqlSelectAux, ngwIndicehgwSqlSelectAux )
--	! ! si la lista esta vacia es un precio de cliente
--	If sLista_de_precios = ''
--		! ! traigo la lista especial del cliente
--		If NOT ObtieneListaPreciosEspeciales(sSucursal,sCliente,sNro_domicilio_cliente,sLista_de_precios )
--			Set swError = swError || 'No se obtubo la lista especial del cliente:' || sCliente ||'
--					'


--	! ! si el envase esta vacio es una actualizacion de precios de un producto
--	If sEnvase = ''
--		! Se va armando las lista de productos para pasar como parametro al dlgPreciosProdDependientes.-
--		Set swListaProductosModificados = swListaProductosModificados || '\'' || sProducto || '\','
--		!
--		Call SqlPrepareAndExecute( hgwSql, swSqlExistePrecioProducto ) EJECTUO LA QUERY DE EXISTE PRECIO PRODUCTO
--		If SqlFetchNext( hgwSql, ngwIndicehgwSql )
--			! ! si hay  precio del producto en precio_producto, lo borro
--			If NOT SqlPrepareAndExecute( hgwSql, swSqlDeletePrecioProducto ) EJECUTO LA QUERY DELETE PRECIO PRODUCTO
--				Set swError = swError || 'No se pudo borrar en precio_producto:' || sProducto || 'lista:' ||sLista_de_precios||'
--						'
--		FUNCION A BUSCAR	If NOT GrabarPistaAuditoria( 'ALTO', 'DPREPRO',nPrecioAntes,sLista_de_precios,sProducto,SalNumberToStrX(nPrecioAntes,12), '', '', '', '', '', '', '')
--				Set swError = swError || 'No se pudo grabar auditoria de borrado de precio, producto:'||sProducto|| 'lista:' ||sLista_de_precios||'
--						'
--		! ! inserto el nuevo  precio del producto en precio_producto
--		If NOT SqlPrepareAndExecute( hgwSql, swSqlInsertPrecioProducto ) TENGO QUE INSERTAR PRECIO PRODUCTO
--			Set swError = swError || 'No se pudo insertar en precio_producto:' || sProducto || 'lista:' ||sLista_de_precios||'
--					'
--		! ! inserto una pista de auditoria por la insercion del nuevo  precio del producto en precio_producto
--		If NOT GrabarPistaAuditoria( 'ALTO', 'ALTAPPTA',0,sProducto,SalNumberToStrX(nPrecio,12),sLista_de_precios, sOperador, SalNumberToStrX(nNumero_de_cambio,0), sSucursal, '', '', '', '')
--			Set swError = swError || 'No se pudo grabar auditoria de insercion de precio, producto:'||sProducto|| 'lista:' ||sLista_de_precios||'
--					'
--		! ! actualizo el precio del producto como procesado
--		If NOT SqlPrepareAndExecute( hgwSql, swSqlUpdateCambio ) EJECUTO LA QUERY UPDATE CAMBIO
--			Set swError = swError || 'No se pudo actualizar cambios_de_precios:' || sProducto || 'lista:' ||sLista_de_precios||'
--			'


--	Else
--		! Se va armando las lista de productos-envases para pasar como parametro al dlgPreciosProdEnvDependientes.-
--		Set swListaProductosEnvModificados = swListaProductosEnvModificados || '\'' || sProducto || '-' || sEnvase || '\','
--		!
--		Call SqlPrepareAndExecute( hgwSql, swSqlExistePrecioProductoEnv ) EJECUTO EL QUERY DE EXISTE PRECIO PRODUCTO ENVASE
--		If SqlFetchNext( hgwSql, ngwIndicehgwSql )
--			! ! si hay  precio del producto-envase en precio_producto_envase, lo borro
--			If NOT SqlPrepareAndExecute( hgwSql, swSqlDeletePrecioProdEnv ) EJECUTO LA QUERY PRECIO PRODUCTO ENVASE
--				Set swError = swError || 'No se pudo borrar en precio_producto:' || sProducto ||' envase:'|| sEnvase|| ' lista:' ||sLista_de_precios||'
--						'
--			If NOT GrabarPistaAuditoria( 'ALTO', 'DPREPROENV', nPrecioAntes,sLista_de_precios, sProducto, sEnvase,SalNumberToStrX(nPrecioAntes,12), '', '', '', '', '', '')
--				Set swError = swError || 'No se pudo grabar auditoria de borrado de precio, producto:' || sProducto ||' envase:'|| sEnvase|| ' lista:' ||sLista_de_precios||'
--						'
--		! ! inserto el nuevo  precio del producto en precio_producto
--		If NOT SqlPrepareAndExecute( hgwSql, swSqlInsertPrecioProdEnv ) TENGO QUE INSERTAR EL PRECIO PRODUCTO ENVASE
--			Set swError = swError || 'No se pudo insertar en precio_producto:' || sProducto ||' envase:'|| sEnvase|| ' lista:' ||sLista_de_precios||'
--					'
--		! ! inserto una pista de auditoria por la insercion del nuevo  precio del producto-envase en precio_producto_envase
--		If NOT GrabarPistaAuditoria( 'ALTO', 'ALTAPPETA',0,sProducto, sEnvase, SalNumberToStrX(nPrecio,12),sLista_de_precios, sOperador, SalNumberToStrX(nNumero_de_cambio,0), sSucursal, '', '', '')
--			Set swError = swError || 'No se pudo grabar auditoria de insercion de precio, producto:'||sProducto||  'envase:' ||sEnvase||  'lista:' ||sLista_de_precios||'
--					'
--		! ! actualizo el precio del producto como procesado
--		If NOT SqlPrepareAndExecute( hgwSql, swSqlUpdateCambio ) EJECUTO LA QUERY UPDATE CAMBIO
--			Set swError = swError || 'No se pudo actualizar cambios_de_precios:' || sProducto ||' envase:'|| sEnvase|| ' lista:' ||sLista_de_precios||'
--					'





--- ======================TODO ESTO TAMBIEN SE DEBE DE EJECUTAR?? ================

--! Se quitan las comas finales a las listas de productos y productos/envases, respectivamente.-
--Set swListaProductosModificados = SalStrLeftX( swListaProductosModificados, SalStrLength( swListaProductosModificados ) - 1 )
--Set swListaProductosEnvModificados = SalStrLeftX( swListaProductosEnvModificados, SalStrLength( swListaProductosEnvModificados ) - 1 )
--!
--Call SalWaitCursor( FALSE )
--! !
--! Cierro la transaccion
--If swError = ''
--	If NOT SqlPrepareAndExecute( hgwSql, 'COMMIT TRANSACTION' )
--		Set swError = swError || 'No se pudo hacer el commit.
--				'
--Else
--	If NOT SqlPrepareAndExecute( hgwSql, 'ROLLBACK TRANSACTION' )
--		Set swError = swError || 'No se pudo hacer el rollback.
--				'
--!
--! Defino el resultado de la funcion, lo notifico e inserto la historia.
--If swError = ''
--	! ! actualizo los precios porcentajes
--	Call SalModalDialog( dlgPreciosProdDependientes, hWndForm, swListaProductosModificados ) -- BUSCAR EN PROYECTO_96 MEDIANTE EL DLG, No hay nada en linde chile, messer chile y colombia. Probablemente sea otro tipo de funcion, en centura tampoco invocan otra sentencia
--	Call SalModalDialog( dlgPreciosProdEnvDependientes, hWndForm, swListaProductosEnvModificados )
--	Set bwResultado = TRUE
--Else
--	Set bwResultado = FALSE
--!
--! Mando el mail
--Set swTo = HallarDestinatariosMail( spSucursal, spCodigo_de_tarea, bwResultado )
--If swTo != ''
--	If NOT bwResultado
--		Call MandarMail( spSucursal, spCodigo_de_tarea, swTo, 'Sucursal ' || spSucursal || ' - Error en la tarea ' || spCodigo_de_tarea, swError, '' )
--	Else
--		Call MandarMail( spSucursal, spCodigo_de_tarea, swTo, 'Sucursal ' || spSucursal || ' - Ok tarea ' || spCodigo_de_tarea, 'Actualizacion de Precios OK', '' )
--!
--Return bwResultado





--IF @@ROWCOUNT = 0
--                           BEGIN
--                                  -- Se setean valores para el log
--                                  SET @sLogType       = 'ERROR'; 
--                                  SET @sStepType             = 'VALIDACION';
--                                  SET @sStepName             = 'VALIDACION DE REGISTRO'; 
--                                  SET @nErrNumber            = 0;
--                                  SET @nErrSeverity   = 0;
--                                  SET @nErrState             = 0;
--                                  SET @nSpErrLine     = 0;
--                                  SET @sMessage       = 'NO EXISTE DOCUMENTO (tipo_documento: ' + 
--                                                                      isNull(@spTipoDocumento,'NULO') + ', num_documento: ' + 
--                                                                      isNull(@spNumeroDocumento,'NULO') + ', sucursal: ' +
--                                                                      isNull(@spSucursal,'NULO') + ', clase_nota: ' +
--                                                                      isNull(@spClaseNota,'NULO') + ')';   

--                                  -- Se inserta log
--                                  EXECUTE @nRc = [dbo].[sp_jde_logs]  
--                                                             @sInterfaceName,
--                                                             @sStepType,
--                                                             @sStepName,
--                                                             @sLogType,
--                                                             @nErrNumber,
--                                                             @nErrSeverity,
--                                                             @nErrState,
--                                                             @nSpErrLine,
--                                                             @sMessage,
--                                                             @sDestination
                                                             
--                                  -- Se indica que hubo un error
--                                  SET @bError = 1;
--                           END