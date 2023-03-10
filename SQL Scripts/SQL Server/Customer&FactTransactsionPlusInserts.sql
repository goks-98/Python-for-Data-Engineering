USE [AdventureWorks2019]
GO
DROP TABLE [dbo].[customers]
GO
/****** Object:  Table [dbo].[customers]    Script Date: 4/4/2022 3:29:25 PM ******/
CREATE TABLE [dbo].[customers](
	[customerId] [int] NOT NULL,
	[customername] [varchar](200) NOT NULL,
	[customertype] [varchar](20) NULL,
	[entrydate] [datetime] NOT NULL,
	[created_at] [datetime] NULL,
	[modified_at] [datetime] NULL,
 CONSTRAINT [PK_customers_pk] PRIMARY KEY CLUSTERED ([customerId] ASC)
) 

GO

INSERT [dbo].[customers] ([customerId], [customername], [customertype], [entrydate], [created_at], [modified_at]) 
VALUES (1, N'Michael Scott', N'Corporate', GETDATE(), GETDATE(), GETDATE())
INSERT [dbo].[customers] ([customerId], [customername], [customertype], [entrydate], [created_at], [modified_at]) 
VALUES (2, N'Dwight Schrute', N'Individual', GETDATE(), GETDATE(), GETDATE())
INSERT [dbo].[customers] ([customerId], [customername], [customertype], [entrydate], [created_at], [modified_at]) 
VALUES (3, N'Jim Halpert', N'Individual', GETDATE(), GETDATE(), GETDATE())