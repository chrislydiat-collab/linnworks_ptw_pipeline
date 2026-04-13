USE [linnworks]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[ProductKeyGuidMap](
	[ProductKey] [nvarchar](255) NOT NULL,
	[ProductKeyGuid] [uniqueidentifier] NOT NULL,
	[pkStockID] [uniqueidentifier] NOT NULL,
	[pkOrderID] [uniqueidentifier] NOT NULL,
	[pkStockItemID] [uniqueidentifier] NOT NULL,
 CONSTRAINT [PK_ProductKeyGuidMap] PRIMARY KEY CLUSTERED 
(
	[ProductKey] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO


