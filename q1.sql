-- Databricks notebook source
use WideWorldImporters;

select top (10) c.CustomerID, c.CustomerName, sum(Quantity) as total_Quantity

from Sales.OrderLines as ol
inner join Sales.Orders as o on o.OrderID=ol.OrderID
inner join Sales.Customers as c on c.CustomerID =o.CustomerID
group by c.CustomerID, c. CustomerName
Order by total_Quantity desc;


-- use only customerID in the group by and make it a cte, then,  inner join CustomerName to the cte created before.
