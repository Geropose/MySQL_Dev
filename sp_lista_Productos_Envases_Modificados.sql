USE [MESSER_CHILE]
GO
/****** Object:  StoredProcedure [dbo].[sp_lista_Productos_Envases_Modificados]    Script Date: 03-02-2023 12:43:25 ******/
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

ALTER PROCEDURE [dbo].[sp_lista_Productos_Envases_Modificados]
( 
	   @sSucursal						VARCHAR(10),
	   @swListaProductosEnvModificados	VARCHAR(MAX),
	   @sLista_de_precios				VARCHAR(MAX)
)  
AS
SET NOCOUNT ON;

 BEGIN

	SET NOCOUNT ON;
		DECLARE 
			@cantNiveles					INT,
			@nPrecio_producto_envase_base	DECIMAL(25,12),
			@cngwDecimalesPrecios			VARCHAR(10),
			@ngwDecimalesPrecios			DECIMAL(10,2),
			@sLista_de_precio				VARCHAR(MAX), 
			@sProducto						VARCHAR(10),
			@sEnvase						VARCHAR(10), 
			@sLista_de_precios_base			VARCHAR(MAX),
			@nPorcentaje_de_la_lista_base	DECIMAL(7,4)

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

		SET @cantNiveles = 0
		SET @LogEnabled = 'TRUE'
		SET @STEP_TYPE = 'Stored Procedure'
		SET @STEP_NAME = '[sp_lista_Productos_Envase_Modificados]' 
		SET @LOG_TYPE = 'INFO' 
		SET @ERR_NUMBER = 0
		SET @ERR_SEVERITY = 0
		SET @ERR_STATE = 0
		SET @SP_ERR_LINE = 0
		SET @MESSAGE = NULL
		SET @DESTINATION = 'Lista de productos modificados'
		SET @ROWID_ERROR = ''
		SET @CANT_ERRORES = 0

		UPDATE 
			precio_producto_envase
		SET 
			precio_producto_envase.precio_producto_envase = NULL
		WHERE 
			( precio_producto_envase.sucursal = @sSucursal ) AND 
			( precio_producto_envase.valor_o_porcentaje_lista = 'P' ) AND 
			precio_producto_envase.producto + '-' + precio_producto_envase.envase IN (@swListaProductosEnvModificados)

	WHILE (@cantNiveles <= 3)
		BEGIN
			DECLARE cinsert CURSOR GLOBAL
		
				FOR  SELECT 
						precio_producto_envase.sucursal, 
						precio_producto_envase.lista_de_precio, 
						precio_producto_envase.producto, 
						precio_producto_envase.envase, 
						precio_producto_envase.lista_de_precios_base, 
						precio_producto_envase.porcentaje_de_la_lista_base 

					FROM 
						precio_producto_envase 

					WHERE 
						( precio_producto_envase.precio_producto_envase IS NULL  ) AND 
						( precio_producto_envase.sucursal = @sSucursal ) AND 
						( precio_producto_envase.valor_o_porcentaje_lista = 'P' ) AND 
						  precio_producto_envase.producto + '-' + precio_producto_envase.envase IN (@swListaProductosEnvModificados)
				
				OPEN cinsert

				FETCH cinsert INTO   @sSucursal, @sLista_de_precio, @sProducto, @sEnvase, @sLista_de_precios_base, @nPorcentaje_de_la_lista_base

				WHILE(@@fetch_status < 1)
					BEGIN
						BEGIN TRY
							BEGIN TRANSACTION
								SET @ngwDecimalesPrecios = (SELECT 
																Parametros_generales.Valor
															FROM
																Parametros_generales
															WHERE
																(Parametros_generales.sucursal = @sSucursal) AND
																(Parametros_generales.Variable = 'ngwDecimalesPrecios'))
								SET @nPrecio_producto_envase_base = (
																		SELECT 
																			precio_producto_envase.precio_producto_envase 

																		FROM 
																			precio_producto_envase 

																		WHERE 
																			(precio_producto_envase.sucursal = @sSucursal) AND 
																			(precio_producto_envase.lista_de_precio = @sLista_de_precios_base) AND 
																			(precio_producto_envase.producto = @sProducto) AND 
																			(precio_producto_envase.envase = @sEnvase))

								If (@nPrecio_producto_envase_base != NULL)
										UPDATE 
											precio_producto_envase

										SET 
											precio_producto_envase.precio_producto_envase = ROUND( @nPrecio_producto_envase_base * ( 1 + @nPorcentaje_de_la_lista_base / 100 ),  @ngwDecimalesPrecios)

										WHERE 
											( precio_producto_envase.sucursal = @sSucursal ) AND 
											( precio_producto_envase.lista_de_precio = @sLista_de_precio ) AND  
											( precio_producto_envase.producto = @sProducto ) AND 
											( precio_producto_envase.envase = @sEnvase ) 
							COMMIT TRANSACTION

						FETCH cinsert INTO   @sSucursal, @sLista_de_precio, @sProducto, @sLista_de_precios_base, @nPorcentaje_de_la_lista_base
						END TRY
							BEGIN CATCH
								ROLLBACK TRANSACTION
								SET @STEP_TYPE = 'Lista de productos envase modificados'
								SET @STEP_NAME = '[sp_lista_Productos_Envases_Modificados]' 
								SET @LOG_TYPE = 'ERROR' 
								SET @ERR_NUMBER = ERROR_NUMBER();
								SET @ERR_SEVERITY = ERROR_SEVERITY();
								SET @ERR_STATE = ERROR_STATE();
								SET @SP_ERR_LINE = ERROR_LINE();
								SET @MESSAGE = 'ERROR EN LAS TRANSACCIONES INSERT/UPDATE de la lista de precio: ' + @sLista_de_precio + '. Error:' +  ERROR_MESSAGE()
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
			SET @cantNiveles += 1;
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
	SET @STEP_TYPE='[sp_lista_Productos_Envases_Modificados]'
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