USE [MESSER_CHILE]
GO
/****** Object:  StoredProcedure [dbo].[sp_HallarDestinatario]    Script Date: 03-02-2023 12:43:25 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- ===============================================================================
-- Author:		EDSA - Geronimo Pose
-- Create date: 03-02-2023 15:54:25
-- Description: Dicho sp se requiere para cambios de precios
-- ===============================================================================

---- ==========================================================================================
---- Create date: 03/02/2023
---- OBJETIVO: El fin de este stored procedure es hallar los mails de los destinatarios y poder
---- enviar notificaciones
---- ==========================================================================================

ALTER PROCEDURE [dbo].[sp_HallarDestinatario]
( 
	   @sSucursal varchar(10),
	   @sCodigo_de_tarea varchar(20), 
	   @sLista_Direccion varchar(MAX), 
	   @sErrorEjecucion bit
)  
AS
SET NOCOUNT ON;
 BEGIN
	SET NOCOUNT ON;
		DECLARE @sDireccion VARCHAR(50) = ''
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
		SET @STEP_NAME = '[sp_HallarDestinatario]' 
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
				BEGIN TRY
					BEGIN TRANSACTION
						 IF (@sErrorEjecucion = 1)
								SET @sDireccion = (SELECT 
														 direcciones_de_mail.direccion
   
														 FROM 
														 resultado_exitoso, 
														 direcciones_de_mail
   
														 WHERE 
														 direcciones_de_mail.sucursal = resultado_exitoso.sucursal AND 
														 direcciones_de_mail.codigo_de_direccion = resultado_exitoso.codigo_de_direccion AND 
														 resultado_exitoso.sucursal = @sSucursal AND 
														 resultado_exitoso.codigo_de_tarea = @sCodigo_de_tarea)
						ELSE
						 	SET @sDireccion = (SELECT 
						   							direcciones_de_mail.direccion

						   							FROM 
						   							resultado_con_error, 
						   							direcciones_de_mail

						   							WHERE 
						   							direcciones_de_mail.sucursal = resultado_con_error.sucursal AND 
						   							direcciones_de_mail.codigo_de_direccion = resultado_con_error.codigo_de_direccion AND 
						   							resultado_con_error.sucursal = @sSucursal AND 
						   							resultado_con_error.codigo_de_tarea = @sCodigo_de_tarea)

						DECLARE clistamail CURSOR GLOBAL
						--------------------------- SELECT CURSOR DE LAS DIRECCIONES DE MAILS ---------------------------
						FOR SELECT 
								direcciones_de_mail.direccion

								FROM 
								resultado_con_error, 
								direcciones_de_mail

								WHERE 
								direcciones_de_mail.sucursal = resultado_con_error.sucursal AND 
								direcciones_de_mail.codigo_de_direccion = resultado_con_error.codigo_de_direccion AND 
								resultado_con_error.sucursal = @sSucursal AND 
								resultado_con_error.codigo_de_tarea = (@sCodigo_de_tarea)

								OPEN clistamail
								FETCH clistamail INTO @sDireccion
								WHILE(@@fetch_status=0)
									BEGIN
										SET @sLista_Direccion = @sLista_Direccion + '''' +  @sDireccion + '''' + ',' 
										FETCH clistamail INTO @sDireccion
									END
								CLOSE clistamail
							DEALLOCATE clistamail

					SET @sLista_Direccion = (SELECT LEFT(@sLista_Direccion, LEN(@sLista_Direccion)-1))
					COMMIT TRANSACTION
				END TRY
					BEGIN CATCH
						ROLLBACK TRANSACTION
						SET @STEP_TYPE = 'Envio de mail'
						SET @STEP_NAME = '[sp_HallarDestinatario]' 
						SET @LOG_TYPE = 'ERROR' 
						SET @ERR_NUMBER = ERROR_NUMBER();
						SET @ERR_SEVERITY = ERROR_SEVERITY();
						SET @ERR_STATE = ERROR_STATE();
						SET @SP_ERR_LINE = ERROR_LINE();
						SET @MESSAGE = 'ERROR EN EL ENVIO DE MAIL DE CAMBIO DE PRECIO, SUCURSAL: ' + @sSucursal + 'CON CODIGO DE TAREA: ' + @sCodigo_de_tarea + '. Error:' +  ERROR_MESSAGE()
						SET @DESTINATION = 'SP'

						EXECUTE @RC = [dbo].[sp_Cambio_Precios_Logs]
								   @STEP_TYPE ,@STEP_NAME ,@LOG_TYPE ,@ERR_NUMBER ,@ERR_SEVERITY ,@ERR_STATE ,@SP_ERR_LINE ,@MESSAGE,@DESTINATION
						
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
  SET @STEP_TYPE='[sp_HallarDestinatario]'
  SET @ERR_NUMBER = 0 ; 
  SET @ERR_SEVERITY = 0 ; 
  SET @ERR_STATE = ''; 
  SET @SP_ERR_LINE = '' ; 
  SET @DESTINATION = 'SP';
  
		
	EXECUTE @RC = [dbo].[sp_Cambio_Precios_Logs]
				@STEP_TYPE ,@STEP_NAME ,@LOG_TYPE ,@ERR_NUMBER ,@ERR_SEVERITY ,@ERR_STATE ,@SP_ERR_LINE ,@MESSAGE,@DESTINATION
				
END