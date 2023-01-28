USE AdventureWorks2019
GO
--insert
INSERT [dbo].[customers] ( [customerid], [customername], [customertype], [entrydate], [created_at], [modified_at]) 
VALUES (4, N'Pam Halpert', N'Individual', GETDATE(), GETDATE(), GETDATE()),
       (5, N'Stanely Hudson', N'Individual', GETDATE(), GETDATE(), GETDATE())

--update
UPDATE dbo.customers
SET [customertype] = 'Corporate', [modified_at] = GETDATE()
WHERE customerId = 3