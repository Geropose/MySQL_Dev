USE [MESSER_CHILE]
GO

/****** Object:  Table [dbo].[Cambio_Precios_Logs]    Script Date: 03/03/2023 12:30:19 PM ******/

-- ===============================================================================
-- Author:		EDSA - Geronimo Pose
-- Create date: 09-02-2023 12:30:19
-- Description:	Se crea la tabla para guardar los logs de cambio de precios.
-- ===============================================================================

SET ANSI_NULLS OFF
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[Cambio_Precios_Logs](
						[LOG_TIME] [datetime] NULL,
						[STEP_TYPE] [varchar](500) NULL,
						[STEP_NAME] [varchar](500) NULL,
						[LOG_TYPE] [varchar](100) NULL,
						[ERR_NUMBER] [int] NULL,
						[ERR_SEVERITY] [int] NULL,
						[ERR_STATE] [int] NULL,
						[SP_ERR_LINE] [int] NULL,
						[MESSAGE] [varchar](4000) NULL,
						[DESTINATION] [varchar](500) NULL
					) ON [PRIMARY]
GO


