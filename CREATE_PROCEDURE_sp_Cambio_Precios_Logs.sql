USE [MESSER_CHILE]
GO
/****** Object:  StoredProcedure [dbo].[sp_Cambio_Precios_Logs]    Script Date: 03/03/2023 12:35:05 PM ******/

-- ===============================================================================
-- Author:		EDSA - Geronimo Pose
-- Create date: 03-03-2023 12:35:05
-- Description:	Se crea el SP que se encarga de guardar los logs de cada error de cambio de precios.
-- ===============================================================================

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

	CREATE PROCEDURE [dbo].[sp_Cambio_Precios_Logs]
				@STEP_TYPE as varchar(500) = NULL,
				@STEP_NAME as varchar(500) = NULL,
				@LOG_TYPE as varchar(100) = NULL,
				@ERR_NUMBER as int = NULL,
				@ERR_SEVERITY as int = NULL,
				@ERR_STATE as int = NULL,
				@SP_ERR_LINE as int = NULL,
				@MESSAGE as varchar(4000) = NULL,
				@DESTINATION as varchar(500) = NULL
	AS
	BEGIN
		INSERT INTO [dbo].[Cambio_Precios_Logs]
			   ([LOG_TIME]
			   ,[STEP_TYPE]
			   ,[STEP_NAME]
			   ,[LOG_TYPE]
			   ,[ERR_NUMBER]
			   ,[ERR_SEVERITY]
			   ,[ERR_STATE]
			   ,[SP_ERR_LINE]
			   ,[MESSAGE]
			   ,[DESTINATION]
			   )
		 VALUES
			   (GETDATE()
			   ,@STEP_TYPE
			   ,@STEP_NAME
			   ,@LOG_TYPE
			   ,@ERR_NUMBER
			   ,@ERR_SEVERITY
			   ,@ERR_STATE
			   ,@SP_ERR_LINE
			   ,@MESSAGE
			   ,@DESTINATION
			   )
	END
