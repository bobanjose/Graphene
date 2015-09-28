/****** Object:  UserDefinedTableType [dbo].[FilterList]    Script Date: 9/15/2015 10:33:18 AM ******/
CREATE TYPE [dbo].[FilterList] AS TABLE(
	[Filter] [nvarchar](450) NULL
)
GO
/****** Object:  UserDefinedTableType [dbo].[Measurement]    Script Date: 9/15/2015 10:33:18 AM ******/
CREATE TYPE [dbo].[Measurement] AS TABLE(
	[Name] [nvarchar](100) NULL,
	[Value] [bigint] NULL,
	[BucketResolution] [int] NULL,
	[CoversMinuteBucket] [bit] NULL,
	[CoversFiveMinuteBucket] [bit] NULL,
	[CoversFifteenMinuteBucket] [bit] NULL,
	[CoversThirtyMinuteBucket] [bit] NULL,
	[CoversHourBucket] [bit] NULL,
	[CoversDayBucket] [bit] NULL,
	[CoversMonthBucket] [bit] NULL
)
GO
/****** Object:  UserDefinedTableType [dbo].[TypeName]    Script Date: 9/15/2015 10:33:20 AM ******/
CREATE TYPE [dbo].[TypeName] AS TABLE(
	[TypeName] [nvarchar](200) NULL
)
GO
/****** Object:  Table [dbo].[BucketTrackerTypes]    Script Date: 9/15/2015 10:33:20 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[BucketTrackerTypes](
	[TrackerTypeId] [uniqueidentifier] NOT NULL,
	[TrackerResolution] [int] NULL,
 CONSTRAINT [BucketTrackerTypes_PK] PRIMARY KEY CLUSTERED 
(
	[TrackerTypeId] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF)
)

GO
/****** Object:  Table [dbo].[FilterGroupFilters]    Script Date: 9/15/2015 10:33:20 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[FilterGroupFilters](
	[FilterGroupFiltersId] [uniqueidentifier] NOT NULL DEFAULT (newsequentialid()),
	[FilterGroupId] [uniqueidentifier] NOT NULL DEFAULT (newsequentialid()),
	[FilterGroupKeyFilter] [nvarchar](400) NULL,
	[FilterGroupFilterString] [nvarchar](400) NULL,
	[IsNullFilterGroupFilterString]  AS (isnull([FilterGroupFilterString],'')),
 CONSTRAINT [PK_FilterGroupFilters] PRIMARY KEY CLUSTERED 
(
	[FilterGroupFiltersId] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF)
)

GO
/****** Object:  Table [dbo].[FilterGroups]    Script Date: 9/15/2015 10:33:20 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[FilterGroups](
	[FilterGroupID] [uniqueidentifier] NOT NULL CONSTRAINT [DF_FilterGroups_FilterGroupID]  DEFAULT (newsequentialid()),
	[FilterGroupKeyFilter] [nvarchar](400) NULL,
 CONSTRAINT [PK_FilterGroups] PRIMARY KEY CLUSTERED 
(
	[FilterGroupID] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF)
)

GO
/****** Object:  Table [dbo].[ProcessingLog]    Script Date: 9/15/2015 10:33:20 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ProcessingLog](
	[SessionId] [uniqueidentifier] NOT NULL,
	[ApplicationName] [varchar](50) NOT NULL,
	[MessageDate] [datetime] NOT NULL,
	[MessageText] [varchar](500) NULL
)

GO
/****** Object:  Table [dbo].[SessionFilterLists]    Script Date: 9/15/2015 10:33:20 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[SessionFilterLists](
	[SessionId] [uniqueidentifier] NOT NULL,
	[FilterType] [int] NOT NULL,
	[FilterValue] [nvarchar](440) NOT NULL,
	[SessionExpiryDate] [datetime] NOT NULL DEFAULT (sysdatetime()),
	[IsNullFilterValue]  AS (isnull([FilterValue],'')),
 CONSTRAINT [PK_SessionFilterLists] UNIQUE CLUSTERED 
(
	[SessionId] ASC,
	[FilterType] ASC,
	[FilterValue] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = ON)
)

GO
/****** Object:  Table [dbo].[Tracker]    Script Date: 9/15/2015 10:33:20 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Tracker](
	[Id] [uniqueidentifier] NOT NULL DEFAULT (newsequentialid()),
	[TrackerId] [nvarchar](300) NOT NULL,
	[TrackerTypeId] [uniqueidentifier] NOT NULL,
 CONSTRAINT [PK_Tracker] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF)
)

GO
/****** Object:  Table [dbo].[TrackerData]    Script Date: 9/15/2015 10:33:20 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[TrackerData](
	[Id] [uniqueidentifier] NOT NULL DEFAULT (newsequentialid()),
	[TimeSlotDt] [datetime] NOT NULL,
	[TrackerId] [nvarchar](300) NOT NULL,
	[TrackerTypeId] [uniqueidentifier] NOT NULL,
	[TimeSlotStr] [char](19) NOT NULL,
	[KeyFilter] [nvarchar](400) NULL,
	[TrackerMeasureId] [uniqueidentifier] NOT NULL,
	[MeasurementValue] [bigint] NOT NULL,
	[FilterGroupID] [uniqueidentifier] NOT NULL,
	[DataResolution] [bigint] NOT NULL,
 CONSTRAINT [PK_TrackerData] PRIMARY KEY CLUSTERED 
(
	[TimeSlotDt] DESC,
	[TrackerTypeId] ASC,
	[Id] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF)
)

GO
/****** Object:  Table [dbo].[TrackerDocuments]    Script Date: 9/15/2015 10:33:20 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[TrackerDocuments](
	[Tracker_Document_Id] [nvarchar](400) NOT NULL,
	[Tracker_Json_Document] [nvarchar](max) NOT NULL,
	[Tracker_Xml_Document] [xml] NULL,
	[Document_Converted_By_Bulk_Load] [nchar](1) NULL,
 CONSTRAINT [PK_TrackerDocuments] PRIMARY KEY CLUSTERED 
(
	[Tracker_Document_Id] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF)
)

GO
/****** Object:  Table [dbo].[TrackerMeasures]    Script Date: 9/15/2015 10:33:20 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[TrackerMeasures](
	[TrackerMeasureId] [uniqueidentifier] NOT NULL DEFAULT (newsequentialid()),
	[TrackerMeasureName] [varchar](200) NOT NULL,
	[TrackerTypeId] [uniqueidentifier] NOT NULL,
 CONSTRAINT [PK_TrackerMeasures] PRIMARY KEY CLUSTERED 
(
	[TrackerMeasureId] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF)
)

GO
/****** Object:  Table [dbo].[TrackerTypes]    Script Date: 9/15/2015 10:33:20 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[TrackerTypes](
	[TrackerTypeId] [uniqueidentifier] NOT NULL DEFAULT (newsequentialid()),
	[TrackerTypeName] [varchar](150) NOT NULL,
	[TrackerTypeDescription] [varchar](250) NOT NULL,
	[TrackerTypeMinimumResolution] [bigint] NOT NULL,
 CONSTRAINT [PK_TrackerTypes] PRIMARY KEY CLUSTERED 
(
	[TrackerTypeId] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF)
)

GO
/****** Object:  View [dbo].[TrackerData_Vw]    Script Date: 9/15/2015 10:33:20 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE VIEW [dbo].[TrackerData_Vw]
WITH SCHEMABINDING
AS    SELECT   td.MeasurementValue AS MeasurementValue
			 , tm.TrackerMeasureName AS MeasurementName
			 , TT.TrackerTypeName AS TypeName
			 , FilterGroupID As FilterGroupId
			 , KeyFilter as KeyFilter
			 , TimeSlotDt AS MeasureDateTime
			 , dateadd(hour,-1, TimeSlotDt) MeasureDateTimeOffsetMinus1Hour
			 , dateadd(hour,-2, TimeSlotDt) MeasureDateTimeOffsetMinus2Hour
			 , dateadd(hour,-3, TimeSlotDt) MeasureDateTimeOffsetMinus3Hour
			 , dateadd(hour,-4, TimeSlotDt) MeasureDateTimeOffsetMinus4Hour
			 , dateadd(hour,-5, TimeSlotDt) MeasureDateTimeOffsetMinus5Hour
			 , dateadd(hour,-6, TimeSlotDt) MeasureDateTimeOffsetMinus6Hour
			 , dateadd(hour,-7, TimeSlotDt) MeasureDateTimeOffsetMinus7Hour
			 , dateadd(hour,-8, TimeSlotDt) MeasureDateTimeOffsetMinus8Hour
			 , dateadd(hour,-9, TimeSlotDt) MeasureDateTimeOffsetMinus9Hour
			 , dateadd(hour,-10, TimeSlotDt) MeasureDateTimeOffsetMinus10Hour
			 , dateadd(hour,-11, TimeSlotDt) MeasureDateTimeOffsetMinus11Hour
			 , dateadd(hour,-12, TimeSlotDt) MeasureDateTimeOffsetMinus12Hour
			 , dateadd(hour,1, TimeSlotDt) MeasureDateTimeOffsetPlus1Hour
			 , dateadd(hour,2, TimeSlotDt) MeasureDateTimeOffsetPlus2Hour
			 , dateadd(hour,3, TimeSlotDt) MeasureDateTimeOffsetPlus3Hour
			 , dateadd(hour,4, TimeSlotDt) MeasureDateTimeOffsetPlus4Hour
			 , dateadd(hour,5, TimeSlotDt) MeasureDateTimeOffsetPlus5Hour
			 , dateadd(hour,6, TimeSlotDt) MeasureDateTimeOffsetPlus6Hour
 			 , dateadd(hour,7, TimeSlotDt) MeasureDateTimeOffsetPlus7Hour
			 , dateadd(hour,8, TimeSlotDt) MeasureDateTimeOffsetPlus8Hour
			 , dateadd(hour,9, TimeSlotDt) MeasureDateTimeOffsetPlus9Hour
			 , dateadd(hour,10, TimeSlotDt) MeasureDateTimeOffsetPlus10Hour
			 , dateadd(hour,11, TimeSlotDt) MeasureDateTimeOffsetPlus11Hour
			 , dateadd(hour,12, TimeSlotDt) MeasureDateTimeOffsetPlus12Hour
			 , dateadd(hour,13, TimeSlotDt) MeasureDateTimeOffsetPlus13Hour
			 , dateadd(hour,14, TimeSlotDt) MeasureDateTimeOffsetPlus14Hour
       FROM  dbo.TrackerData td 
	   JOIN dbo.TrackerTypes tt
	     ON  tt.TrackerTypeId = td.TrackerTypeId
	   JOIN dbo.TrackerMeasures tm
	     ON tm.TrackerMeasureId = td.TrackerMeasureId
	  WHERE td.DataResolution = tt.TrackerTypeMinimumResolution




GO
SET ANSI_PADDING ON

GO
/****** Object:  Index [IX_FilterGroupFilters_FilterString]    Script Date: 9/15/2015 10:33:20 AM ******/
CREATE NONCLUSTERED INDEX [IX_FilterGroupFilters_FilterString] ON [dbo].[FilterGroupFilters]
(
	[FilterGroupFilterString] ASC,
	[FilterGroupId] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF)
GO
SET ARITHABORT ON
SET CONCAT_NULL_YIELDS_NULL ON
SET QUOTED_IDENTIFIER ON
SET ANSI_NULLS ON
SET ANSI_PADDING ON
SET ANSI_WARNINGS ON
SET NUMERIC_ROUNDABORT OFF

GO
/****** Object:  Index [IX_FilterGroupFilters_Fn]    Script Date: 9/15/2015 10:33:20 AM ******/
CREATE NONCLUSTERED INDEX [IX_FilterGroupFilters_Fn] ON [dbo].[FilterGroupFilters]
(
	[IsNullFilterGroupFilterString] ASC,
	[FilterGroupId] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF)
GO
SET ANSI_PADDING ON

GO
/****** Object:  Index [IX_FilterGroupFilters_KeyFilter]    Script Date: 9/15/2015 10:33:20 AM ******/
CREATE NONCLUSTERED INDEX [IX_FilterGroupFilters_KeyFilter] ON [dbo].[FilterGroupFilters]
(
	[FilterGroupKeyFilter] ASC,
	[FilterGroupId] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF)
GO
SET ANSI_PADDING ON

GO
/****** Object:  Index [UX_FilterGroups_KeyFilter_GroupID]    Script Date: 9/15/2015 10:33:20 AM ******/
CREATE UNIQUE NONCLUSTERED INDEX [UX_FilterGroups_KeyFilter_GroupID] ON [dbo].[FilterGroups]
(
	[FilterGroupKeyFilter] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF)
GO
SET ANSI_PADDING ON

GO
/****** Object:  Index [IX_ProcessingLog_Type_Date_Id]    Script Date: 9/15/2015 10:33:20 AM ******/
CREATE NONCLUSTERED INDEX [IX_ProcessingLog_Type_Date_Id] ON [dbo].[ProcessingLog]
(
	[MessageDate] ASC,
	[ApplicationName] ASC,
	[SessionId] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF)
GO
SET ARITHABORT ON
SET CONCAT_NULL_YIELDS_NULL ON
SET QUOTED_IDENTIFIER ON
SET ANSI_NULLS ON
SET ANSI_PADDING ON
SET ANSI_WARNINGS ON
SET NUMERIC_ROUNDABORT OFF

GO
/****** Object:  Index [IX_SessionFilterLists_Fn]    Script Date: 9/15/2015 10:33:20 AM ******/
CREATE NONCLUSTERED INDEX [IX_SessionFilterLists_Fn] ON [dbo].[SessionFilterLists]
(
	[SessionId] ASC,
	[IsNullFilterValue] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF)
GO
SET ANSI_PADDING ON

GO
/****** Object:  Index [UX_Tracker_TrackerID]    Script Date: 9/15/2015 10:33:20 AM ******/
CREATE UNIQUE NONCLUSTERED INDEX [UX_Tracker_TrackerID] ON [dbo].[Tracker]
(
	[TrackerId] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF)
GO
SET ANSI_PADDING ON

GO
/****** Object:  Index [IX_TrackerData_FG_TT_TimeSlotDt]    Script Date: 9/15/2015 10:33:20 AM ******/
CREATE NONCLUSTERED INDEX [IX_TrackerData_FG_TT_TimeSlotDt] ON [dbo].[TrackerData]
(
	[FilterGroupID] ASC,
	[TrackerTypeId] ASC,
	[TimeSlotDt] DESC
)
INCLUDE ( 	[TimeSlotStr],
	[TrackerMeasureId],
	[MeasurementValue],
	[KeyFilter],
	[DataResolution],
	[TrackerId]) WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF)
GO
SET ANSI_PADDING ON

GO
/****** Object:  Index [IX_TrackerDocuments_Processed]    Script Date: 9/15/2015 10:33:20 AM ******/
CREATE NONCLUSTERED INDEX [IX_TrackerDocuments_Processed] ON [dbo].[TrackerDocuments]
(
	[Document_Converted_By_Bulk_Load] ASC,
	[Tracker_Document_Id] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF)
GO
SET ANSI_PADDING ON

GO
/****** Object:  Index [UX_TrackerTypes_TypeName]    Script Date: 9/15/2015 10:33:20 AM ******/
CREATE NONCLUSTERED INDEX [UX_TrackerTypes_TypeName] ON [dbo].[TrackerTypes]
(
	[TrackerTypeName] ASC
)
INCLUDE ( 	[TrackerTypeMinimumResolution],
	[TrackerTypeId]) WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF)
GO
ALTER TABLE [dbo].[FilterGroupFilters]  WITH CHECK ADD  CONSTRAINT [FK_FilterGroups] FOREIGN KEY([FilterGroupId])
REFERENCES [dbo].[FilterGroups] ([FilterGroupID])
GO
ALTER TABLE [dbo].[FilterGroupFilters] CHECK CONSTRAINT [FK_FilterGroups]
GO
ALTER TABLE [dbo].[Tracker]  WITH CHECK ADD  CONSTRAINT [FK_Tracker_TrackerTypes] FOREIGN KEY([TrackerTypeId])
REFERENCES [dbo].[TrackerTypes] ([TrackerTypeId])
GO
ALTER TABLE [dbo].[Tracker] CHECK CONSTRAINT [FK_Tracker_TrackerTypes]
GO
ALTER TABLE [dbo].[TrackerData]  WITH CHECK ADD  CONSTRAINT [FK_TrackerData_FilterGroups] FOREIGN KEY([FilterGroupID])
REFERENCES [dbo].[FilterGroups] ([FilterGroupID])
GO
ALTER TABLE [dbo].[TrackerData] CHECK CONSTRAINT [FK_TrackerData_FilterGroups]
GO
ALTER TABLE [dbo].[TrackerData]  WITH CHECK ADD  CONSTRAINT [FK_TrackerData_Tracker] FOREIGN KEY([TrackerId])
REFERENCES [dbo].[Tracker] ([TrackerId])
GO
ALTER TABLE [dbo].[TrackerData] CHECK CONSTRAINT [FK_TrackerData_Tracker]
GO
ALTER TABLE [dbo].[TrackerData]  WITH CHECK ADD  CONSTRAINT [FK_TrackerData_TrackerMeasures] FOREIGN KEY([TrackerMeasureId])
REFERENCES [dbo].[TrackerMeasures] ([TrackerMeasureId])
GO
ALTER TABLE [dbo].[TrackerData] CHECK CONSTRAINT [FK_TrackerData_TrackerMeasures]
GO
ALTER TABLE [dbo].[TrackerData]  WITH CHECK ADD  CONSTRAINT [FK_TrackerData_TrackerTypes] FOREIGN KEY([TrackerTypeId])
REFERENCES [dbo].[TrackerTypes] ([TrackerTypeId])
GO
ALTER TABLE [dbo].[TrackerData] CHECK CONSTRAINT [FK_TrackerData_TrackerTypes]
GO
ALTER TABLE [dbo].[TrackerMeasures]  WITH CHECK ADD  CONSTRAINT [FK_TrackerMeasures_TrackerTypes] FOREIGN KEY([TrackerTypeId])
REFERENCES [dbo].[TrackerTypes] ([TrackerTypeId])
GO
ALTER TABLE [dbo].[TrackerMeasures] CHECK CONSTRAINT [FK_TrackerMeasures_TrackerTypes]
GO
ALTER TABLE [dbo].[FilterGroupFilters]  WITH CHECK ADD  CONSTRAINT [FilterGroupsAndStringsAreNullOrNot] CHECK  ((case when [FilterGroupFilterString] IS NOT NULL then (1) else (0) end=case when [FilterGroupKeyFilter] IS NOT NULL then (1) else (0) end))
GO
ALTER TABLE [dbo].[FilterGroupFilters] CHECK CONSTRAINT [FilterGroupsAndStringsAreNullOrNot]
GO
/****** Object:  StoredProcedure [dbo].[GenerateBuckets]    Script Date: 9/15/2015 10:33:20 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[GenerateBuckets] 
   AS 
BEGIN

	SET NOCOUNT ON
	
	DECLARE @resolutions TABLE (bigResolution INT, smallResolution INT)
	DECLARE @trackerTypesToDelete TABLE (TrackerTypeId	UNIQUEIDENTIFIER, TrackerResolution INT)

	DECLARE
		@error				INT,
		@return				INT,
		@RET_OK				INT,
		@DebugMode			INT,
		@biggerResolution	INT,
		@smallerResolution	INT,
		@UnMigratedCount    INT,
		@AllDocumentCount	INT,
		@lockStatus         VARCHAR(400),
		@IHaveTheLock       INT = 0,
		@UTCToLocalMidnightOffset int,
		@UTCTimeZoneMidnightOffset int,
		@DebugSessionId     UniqueIdentifier
	
	SET @DebugSessionId = newid();

	SELECT TOP 1 @lockStatus = messageText FROM ProcessingLog
	 WHERE MessageText LIKE 'Bucket Generation Active%' OR MessageText LIKE 'Bucket Generation Complete%'
	   AND ApplicationName = 'GenerateBuckets'
	ORDER BY messageDate DESC

	IF (@lockStatus LIKE 'Bucket Generation Active%')
	BEGIN
	  INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
			SELECT @DebugSessionId SessionId, 'GenerateBuckets' ApplicationName, SYSDATETIME() MessageDate, cast('Bucket Generation already running.  Exiting ' +  CONVERT(VARCHAR,SYSDATETIME()) as varchar(400)) AS MessageText;
	  GOTO OK_EXIT
	END


	INSERT INTO @resolutions (bigResolution,smallResolution)
		   (SELECT 43200,1440
				UNION
			SELECT 1440, 60
				UNION
			SELECT 60, 30
				UNION
			SELECT 30,15
				UNION 
			SELECT 15, 5
				UNION 
			SELECT 5,1	
			)

   	SET @RET_OK = 0
	SET @DebugMode = 1
	SET @UTCToLocalMidnightOffset = -8
	SET @UTCTimeZoneMidnightOffset = 8
	
	
	INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
		SELECT @DebugSessionId SessionId, 'GenerateBuckets' ApplicationName, SYSDATETIME() MessageDate, cast('Bucket Generation active ' +  CONVERT(VARCHAR,SYSDATETIME()) as varchar(400)) AS MessageText;
	set @IHaveTheLock = 1
	IF (@DebugMode = 1)
	BEGIN
		SELECT 'Locking Bucket Generation ' +  CONVERT(VARCHAR,SYSDATETIME()) AS BucketGenerationLocked;
	END

	INSERT INTO @trackerTypesToDelete SELECT * FROM BucketTrackerTypes;
	
    INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
		SELECT @DebugSessionId SessionId, 'GenerateBuckets' ApplicationName, SYSDATETIME() MessageDate, 'Generating buckets for Tracker Types: ' AS MessageText 
	INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
		SELECT @DebugSessionId SessionId, 'GenerateBuckets' ApplicationName, SYSDATETIME() MessageDate, 'TrackerTypeId: '+ cast(TrackerTypeId as varchar(40)) + '    Resolution: ' + cast(TrackerResolution as varchar(20)) 
				AS MessageText  
		  FROM @trackerTypesToDelete;
	IF (@DebugMode = 1)
	BEGIN
        SELECT * FROM @trackerTypesToDelete
	END;

	IF (@DebugMode = 1)
	BEGIN
    	SELECT 'Removing higher resolution total records at:' +  CONVERT(VARCHAR,SYSDATETIME()) AS ClearingHigherResolutions;
	END
    INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
		SELECT @DebugSessionId SessionId, 'GenerateBuckets' ApplicationName, SYSDATETIME() MessageDate, 'Removing higher resolution total records' AS MessageText 

	BEGIN TRANSACTION
	UPDATE td  SET td.DataResolution = tt.TrackerTypeMinimumResolution
	FROM trackerData td, TrackerTypes tt
    WHERE td.TrackerTypeId= tt.TrackerTypeId
	  AND tt.TrackerTypeMinimumResolution <> 5
	  AND td.DataResolution <> tt.TrackerTypeMinimumResolution
	
	DELETE TD FROM TrackerData td, DeleteTrackerTypes tttd 
	 WHERE td.TrackerTypeId = tttd.TrackerTypeId
       AND td.DataResolution <> tttd.trackerResolution
    COMMIT TRANSACTION

	INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
		SELECT @DebugSessionId SessionId, 'GenerateBuckets' ApplicationName, SYSDATETIME() MessageDate, 'Generating 15 minute total datapoints from 5 minute resolutions' AS MessageText 
	IF (@DebugMode = 1)
	BEGIN
		SELECT 'Generating 15 minute total datapoints from 5 minute resolutions' +  CONVERT(VARCHAR,SYSDATETIME()) AS Adding15MinuteTotals;
	END
	
	BEGIN TRANSACTION
	INSERT INTO TrackerData (TimeSlotDt,MeasurementValue,TrackerId, TrackerTypeId, TimeSlotStr, KeyFilter, TrackerMeasureId, FilterGroupID, DataResolution )
	SELECT CASE WHEN DATEPART(minute,mgm.TimeSlotDt) = 0 THEN mgm.TimeSlotDt
			ELSE DATEADD(mi,(15- ((DATEPART(minute,mgm.TimeSlotDt) - ( 15 *(DATEPART(minute,DATEADD(mi,-1,mgm.TimeSlotDt))/ 15))))),mgm.TimeSlotDt) 
			END Adjusted_15_Minute_date
			, SUM(MeasurementValue) AS MeasurementValue
			, TrackerId, mgm.TrackerTypeId
			, CASE WHEN DATEPART(minute,mgm.TimeSlotDt) = 0 then mgm.TimeSlotDt
			ELSE convert(varchar(20),(DATEADD(mi,(15- ((DATEPART(minute,mgm.TimeSlotDt) - ( 15 *(DATEPART(minute,DATEADD(mi,-1,mgm.TimeSlotDt))/ 15))))),mgm.TimeSlotDt)),120)
			END Adjusted_date_str
			, KeyFilter, mgm.TrackerMeasureId, mgm.FilterGroupID, 15 as DataResolution
		FROM dbo.TrackerData mgm WITH (NOLOCK), @trackerTypesToDelete tttd
		WHERE mgm.DataResolution = 5
		  AND mgm.TrackerTypeId = tttd.TrackerTypeId
		  AND tttd.TrackerResolution <> 15
		GROUP BY CASE WHEN DATEPART(minute,mgm.TimeSlotDt) = 0 THEN mgm.TimeSlotDt
			ELSE DATEADD(mi,(15- ((DATEPART(minute,mgm.TimeSlotDt) - ( 15 *(DATEPART(minute,DATEADD(mi,-1,mgm.TimeSlotDt))/ 15))))),mgm.TimeSlotDt)
			END, TrackerId, mgm.TrackerTypeId, KeyFilter, mgm.TrackerMeasureId, mgm.FilterGroupID, CASE WHEN DATEPART(minute,mgm.TimeSlotDt) = 0 then mgm.TimeSlotDt
			ELSE convert(varchar(20),(DATEADD(mi,(15- ((DATEPART(minute,mgm.TimeSlotDt) - ( 15 *(DATEPART(minute,DATEADD(mi,-1,mgm.TimeSlotDt))/ 15))))),mgm.TimeSlotDt)),120)
			END
		ORDER BY 1
	COMMIT TRANSACTION

	INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
		SELECT @DebugSessionId SessionId, 'GenerateBuckets' ApplicationName, SYSDATETIME() MessageDate, 'Generating 30 minute total datapoints from 15 minute resolutions' AS MessageText 
	IF (@DebugMode = 1)
	BEGIN
    	SELECT 'Generating 30 minute total datapoints from 15 minute resolutions' +  CONVERT(VARCHAR,SYSDATETIME()) AS Generate30MinuteTotals;
	END

	BEGIN TRANSACTION
	INSERT INTO TrackerData (mgm.TimeSlotDt,MeasurementValue,TrackerId, TrackerTypeId, TimeSlotStr, KeyFilter, TrackerMeasureId, FilterGroupID, DataResolution )
	SELECT CASE WHEN DATEPART(minute,mgm.TimeSlotDt) = 0 THEN mgm.TimeSlotDt
			ELSE DATEADD(mi,(30- ((DATEPART(minute,mgm.TimeSlotDt) - ( 30 *(DATEPART(minute,DATEADD(mi,-1,mgm.TimeSlotDt))/ 30))))),mgm.TimeSlotDt) 
			END Adjusted_30_Minute_date
			, SUM(MeasurementValue) AS MeasurementValue
			, TrackerId, mgm.TrackerTypeId
			, CASE WHEN DATEPART(minute,mgm.TimeSlotDt) = 0 then convert(varchar(20),(mgm.TimeSlotDt),120)
			ELSE convert(varchar(20),(DATEADD(mi,(30- ((DATEPART(minute,mgm.TimeSlotDt) - ( 30 *(DATEPART(minute,DATEADD(mi,-1,mgm.TimeSlotDt))/ 30))))),mgm.TimeSlotDt) ),120)
			END Adjusted_date_str
			, KeyFilter, mgm.TrackerMeasureId, mgm.FilterGroupID, 30 as DataResolution
			FROM dbo.TrackerData mgm WITH (NOLOCK), @trackerTypesToDelete tttd
		WHERE mgm.DataResolution = 15
		  AND mgm.TrackerTypeId = tttd.TrackerTypeId
		  AND tttd.TrackerResolution <> 30
		GROUP BY CASE WHEN DATEPART(minute,mgm.TimeSlotDt) = 0 then mgm.TimeSlotDt
						ELSE DATEADD(mi,(30- ((DATEPART(minute,mgm.TimeSlotDt) - ( 30 *(DATEPART(minute,DATEADD(mi,-1,mgm.TimeSlotDt))/ 30))))),mgm.TimeSlotDt) 
					END
				 , TrackerId, mgm.TrackerTypeId, KeyFilter, mgm.TrackerMeasureId, mgm.FilterGroupID,
				 CASE WHEN DATEPART(minute,mgm.TimeSlotDt) = 0 then convert(varchar(20),(mgm.TimeSlotDt),120)
				      ELSE convert(varchar(20),(DATEADD(mi,(30- ((DATEPART(minute,mgm.TimeSlotDt) - ( 30 *(DATEPART(minute,DATEADD(mi,-1,mgm.TimeSlotDt))/ 30))))),mgm.TimeSlotDt) ),120)
					END 
		ORDER BY 1
	COMMIT TRANSACTION

	INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
		SELECT @DebugSessionId SessionId, 'GenerateBuckets' ApplicationName, SYSDATETIME() MessageDate, 'Generating hour total datapoints from 15 minute resolutions' AS MessageText 
	IF (@DebugMode = 1)
	BEGIN
    	SELECT 'Generating hour total datapoints from 15 minute resolutions' +  CONVERT(VARCHAR,SYSDATETIME()) AS GeneratingHourTotals;
	END

	BEGIN TRANSACTION
	INSERT INTO TrackerData (mgm.TimeSlotDt,MeasurementValue,TrackerId, TrackerTypeId, TimeSlotStr, KeyFilter, TrackerMeasureId, FilterGroupID, DataResolution )
		SELECT cast(convert(varchar(13),mgm.TimeSlotDt,120) + ':00' as datetime) Adjusted_date
				, SUM(MeasurementValue) AS MeasurementValue
				, TrackerId, mgm.TrackerTypeId
				, convert(varchar(20),(cast(convert(varchar(13),mgm.TimeSlotDt,120) + ':00' as datetime)),120) Adjusted_date_str
				, KeyFilter, mgm.TrackerMeasureId, mgm.FilterGroupID, 60 as DataResolution
			FROM dbo.TrackerData mgm WITH (NOLOCK), @trackerTypesToDelete tttd
		WHERE mgm.DataResolution = 15
		  AND mgm.TrackerTypeId = tttd.TrackerTypeId
		  AND tttd.TrackerResolution <> 60
		GROUP BY cast(convert(varchar(13),mgm.TimeSlotDt,120) + ':00' as datetime)
 				, TrackerId, mgm.TrackerTypeId, KeyFilter, mgm.TrackerMeasureId, mgm.FilterGroupID,
				convert(varchar(20),(cast(convert(varchar(13),mgm.TimeSlotDt,120) + ':00' as datetime)),120)
		ORDER BY 1
	COMMIT TRANSACTION

	INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
		SELECT @DebugSessionId SessionId, 'GenerateBuckets' ApplicationName, SYSDATETIME() MessageDate, 'Generating day total datapoints from hour resolutions' AS MessageText 
	IF (@DebugMode = 1)
	BEGIN
    	SELECT 'Generating day total datapoints from hour resolutions' +  CONVERT(VARCHAR,SYSDATETIME()) AS GeneratingDayTotals;
	END

	BEGIN TRANSACTION
	INSERT INTO TrackerData (mgm.TimeSlotDt,MeasurementValue,TrackerId, TrackerTypeId, TimeSlotStr, KeyFilter, TrackerMeasureId, FilterGroupID, DataResolution )
		SELECT dateadd(hour,@UTCTimeZoneMidnightOffset,cast(convert(varchar(10),dateadd(hour,@UTCToLocalMidnightOffset, mgm.TimeSlotDt),120) as datetime)) Adjusted_date
				, SUM(MeasurementValue) AS MeasurementValue
				, TrackerId, mgm.TrackerTypeId
				, convert(varchar(20),(dateadd(hour,@UTCTimeZoneMidnightOffset,cast(convert(varchar(10),dateadd(hour,@UTCToLocalMidnightOffset, mgm.TimeSlotDt),120) as datetime))),120) Adjusted_date_str
				, KeyFilter, mgm.TrackerMeasureId, mgm.FilterGroupID, 1440 as DataResolution
			FROM dbo.TrackerData mgm WITH (NOLOCK), @trackerTypesToDelete tttd
		WHERE mgm.DataResolution = 60
		  AND mgm.TrackerTypeId = tttd.TrackerTypeId
		  AND tttd.TrackerResolution <> 1440
	GROUP BY dateadd(hour,@UTCTimeZoneMidnightOffset,cast(convert(varchar(10),dateadd(hour,@UTCToLocalMidnightOffset, mgm.TimeSlotDt),120) as datetime))
	  			, TrackerId, mgm.TrackerTypeId, KeyFilter, mgm.TrackerMeasureId, mgm.FilterGroupID,
			 convert(varchar(20),(dateadd(hour,@UTCTimeZoneMidnightOffset,cast(convert(varchar(10),dateadd(hour,@UTCToLocalMidnightOffset, mgm.TimeSlotDt),120) as datetime))),120)
	ORDER BY 1
	COMMIT TRANSACTION

	INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
		SELECT @DebugSessionId SessionId, 'GenerateBuckets' ApplicationName, SYSDATETIME() MessageDate, 'Generating month total datapoints from day resolutions' AS MessageText 
	IF (@DebugMode = 1)
	BEGIN
    	SELECT 'Generating month total datapoints from day resolutions' +  CONVERT(VARCHAR,SYSDATETIME()) AS GeneratingMonthTotals;
	END
	
	BEGIN TRANSACTION
	INSERT INTO TrackerData (mgm.TimeSlotDt,MeasurementValue,TrackerId, TrackerTypeId, TimeSlotStr, KeyFilter, TrackerMeasureId, FilterGroupID, DataResolution )
			SELECT dateadd(hour,@UTCTimeZoneMidnightOffset,cast(convert(varchar(7),dateadd(hour,@UTCToLocalMidnightOffset, mgm.TimeSlotDt),120) + '-01' as datetime)) Adjusted_date
				, SUM(MeasurementValue) AS MeasurementValue
				, TrackerId, mgm.TrackerTypeId
				, convert(varchar(20),(dateadd(hour,@UTCTimeZoneMidnightOffset,cast(convert(varchar(7),dateadd(hour,@UTCToLocalMidnightOffset, mgm.TimeSlotDt),120)+ '-01' as datetime))),120) Adjusted_date_str
				, KeyFilter, mgm.TrackerMeasureId, mgm.FilterGroupID, 43200 as DataResolution
			FROM dbo.TrackerData mgm WITH (NOLOCK), @trackerTypesToDelete tttd
		WHERE mgm.DataResolution = 1440
		  AND mgm.TrackerTypeId = tttd.TrackerTypeId
		  AND tttd.TrackerResolution <> 43200
		GROUP BY dateadd(hour,@UTCTimeZoneMidnightOffset,cast(convert(varchar(7),dateadd(hour,@UTCToLocalMidnightOffset, mgm.TimeSlotDt),120)+ '-01' as datetime)) 
				, TrackerId, mgm.TrackerTypeId, KeyFilter, mgm.TrackerMeasureId, mgm.FilterGroupID
				, convert(varchar(20),(dateadd(hour,@UTCTimeZoneMidnightOffset,cast(convert(varchar(7),dateadd(hour,@UTCToLocalMidnightOffset, mgm.TimeSlotDt),120)+ '-01' as datetime))),120)
		ORDER BY 1
	COMMIT TRANSACTION

	INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
		SELECT @DebugSessionId SessionId, 'GenerateBuckets' ApplicationName, SYSDATETIME() MessageDate, 'Generating smaller datapoints from bigger resolution datapoints' AS MessageText 
	IF (@DebugMode = 1)
	BEGIN
    	SELECT 'Generating smaller datapoints from bigger resolution datapoints' +  CONVERT(VARCHAR,SYSDATETIME()) AS StartingSmallerResolutionTotals;
	END

	WHILE (SELECT COUNT(*) FROM @RESOLUTIONS) > 0
	BEGIN
		SELECT TOP 1 @biggerResolution = bigResolution, @smallerResolution = smallResolution 
			FROM @resolutions 
		ORDER BY bigResolution DESC

		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
			SELECT @DebugSessionId SessionId, 'GenerateBuckets' ApplicationName, SYSDATETIME() MessageDate, 'Using ' + CAST(@biggerResolution AS varchar) + ' datapoints to generate ' + 
					CAST(@smallerResolution as varchar) + ' datapoints at:' AS MessageText 
		IF (@DebugMode = 1)
		BEGIN
			SELECT 'Using ' + CAST(@biggerResolution AS varchar) + ' datapoints to generate ' + CAST(@smallerResolution as varchar) +
				   ' datapoints at:' +  CONVERT(VARCHAR,SYSDATETIME()) AS GeneratingSmallerPlaceholderTotalRecords;
		END

		BEGIN TRANSACTION
		INSERT INTO TrackerData (TimeSlotDt,MeasurementValue,TrackerId, TrackerTypeId, TimeSlotStr, KeyFilter, TrackerMeasureId, FilterGroupID, DataResolution )
				SELECT TimeSlotDt
					, MeasurementValue
					, TrackerId, td_outer.TrackerTypeId
					, TimeSlotStr
					, KeyFilter, td_outer.TrackerMeasureId, td_outer.FilterGroupID, @smallerResolution as DataResolution
				FROM dbo.TrackerData td_outer WITH (NOLOCK), @trackerTypesToDelete tttd
				WHERE td_outer.DataResolution = @biggerResolution
				  AND td_outer.TrackerTypeId = tttd.TrackerTypeId
				  AND NOT EXISTS (SELECT NULL 
									FROM TrackerData td_inner
								   WHERE td_outer.FilterGroupID = td_inner.FilterGroupID
 									 AND td_outer.KeyFilter = td_inner.KeyFilter
									 AND td_inner.TimeSlotDt BETWEEN td_outer.TimeSlotDt AND CASE WHEN @biggerResolution = 43200 THEN DATEADD(MONTH,1,td_outer.TimeSlotDt)
																								  WHEN @biggerResolution = 1440 THEN DATEADD(DAY,1,td_outer.TimeSlotDt)
																								  WHEN @biggerResolution = 60 THEN DATEADD(HOUR,1,td_outer.TimeSlotDt)
																								  ELSE DATEADD(MINUTE,@biggerResolution,td_outer.TimeSlotDt) 
																							  END
									 AND td_outer.TrackerMeasureId = td_inner.TrackerMeasureId
									 AND td_outer.TrackerTypeId = td_inner.TrackerTypeId
									 AND td_outer.TrackerId = td_inner.TrackerId
									 AND td_inner.DataResolution = @smallerResolution
								)
		COMMIT TRANSACTION

		DELETE FROM @resolutions WHERE bigResolution = @biggerResolution AND smallResolution = @smallerResolution
	END

	DELETE FROM BucketTrackerTypes
	WHERE EXISTS (SELECT NULL FROM @trackerTypesToDelete tttd
					WHERE BucketTrackerTypes.TrackerTypeID = tttd.TrackerTypeId
					  AND BucketTrackerTypes.TrackerResolution= tttd.TrackerResolution)

    INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
		SELECT @DebugSessionId SessionId, 'GenerateBuckets' ApplicationName, SYSDATETIME() MessageDate, 'Rebuilding indexes' AS MessageText 
	IF (@DebugMode = 1)
	BEGIN
		SELECT 'Rebuilding indexes at ' +  CONVERT(VARCHAR,SYSDATETIME()) AS RebuildingIndexes;
	END
	ALTER INDEX [IX_TrackerData] ON [dbo].[TrackerData] REBUILD PARTITION = ALL 
	ALTER INDEX [IX_TrackerData_TrackerType] ON [dbo].[TrackerData] REBUILD PARTITION = ALL 
	ALTER INDEX [PK_TrackerData] ON [dbo].[TrackerData] REBUILD PARTITION = ALL 
	ALTER INDEX [IX_FilterGroupFilters_FilterString] ON [dbo].[FilterGroupFilters] REBUILD PARTITION = ALL 
	ALTER INDEX [IX_FilterGroupFilters_KeyFilter] ON [dbo].[FilterGroupFilters] REBUILD PARTITION = ALL 
	
    INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
		SELECT @DebugSessionId SessionId, 'GenerateBuckets' ApplicationName, SYSDATETIME() MessageDate, 'Index Rebuild finished' AS MessageText 

	IF (@DebugMode = 1)
	BEGIN
		SELECT 'Index Rebuild finished at ' +  CONVERT(VARCHAR,SYSDATETIME()) AS IndexRebuildComplete;
	END

    INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
		SELECT @DebugSessionId SessionId, 'GenerateBuckets' ApplicationName, SYSDATETIME() MessageDate, 'Bucket Generation Complete' AS MessageText 

	IF (@DebugMode = 1)
	BEGIN
		SELECT 'Bucket Generation Complete at ' +  CONVERT(VARCHAR,SYSDATETIME()) AS BucketGenerationComplete;
	END

	SELECT 
		@error = @@ERROR

	IF @error <> 0 
	BEGIN
		GOTO ERR_EXIT
	END
	
	OK_EXIT: 
		IF @return IS NULL SELECT @return = @RET_OK
		SET NOCOUNT OFF
		If (@IHaveTheLock = 1)
		   INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
			 SELECT @DebugSessionId SessionId, 'GenerateBuckets' ApplicationName, SYSDATETIME() MessageDate, 'Bucket Generation Complete' AS MessageText 
		RETURN @return
	
	ERR_EXIT:
		IF (@return IS NULL OR @return = 0) SELECT @return = @error
		If (@IHaveTheLock = 1)
		   INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
			 SELECT @DebugSessionId SessionId, 'GenerateBuckets' ApplicationName, SYSDATETIME() MessageDate, 'Bucket Generation Complete' AS MessageText 
		RETURN @return
END



GO
/****** Object:  StoredProcedure [dbo].[GenerateReport]    Script Date: 9/15/2015 10:33:20 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO





/****** Object:  StoredProcedure [dbo].[GenerateReport]    Script Date: 2/25/2015 10:34:34 AM ******/
--CREATE PROCEDURE [dbo].[GenerateReport] 
CREATE PROCEDURE [dbo].[GenerateReport] 
    @StartDt DATETIME,
	@EndDt DATETIME,
	@Resolution INT,
	@FilterList AS dbo.FilterList READONLY,
	@TypeNameList AS dbo.TypeName READONLY,
	@ExcludeFilterList AS dbo.FilterList READONLY,
	@OffsetTotalsTo AS INT = 8,
	@SessionId uniqueidentifier = NULL
AS 
BEGIN
	
	SET NOCOUNT ON

	DECLARE 
		@error					int,
		@return					int,
		@RET_OK					int,
		@NewSession				int,
		@SessionRecordCount		int,
		@SessionExpiryDate		DateTime,
		@DebugMode              int,
		@DebugId				uniqueidentifier

	SET @DebugMode = 1;
	SET @DebugId = newid()
	
	-- Set Return Values 
	SELECT 
		@RET_OK = 0

	IF (@DebugMode = 1)
	BEGIN
		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
				SELECT @DebugId SessionId, 'GenerateReport' ApplicationName, SYSDATETIME() MessageDate, ' Generate Report called with StartDate : ' +  convert(VARCHAR,@StartDt)  AS MessageText;;

		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
				SELECT @DebugId SessionId, 'GenerateReport' ApplicationName, SYSDATETIME() MessageDate, '                             EndDate : ' +  convert(VARCHAR,@EndDt)  AS MessageText;
		
		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
				SELECT @DebugId SessionId, 'GenerateReport' ApplicationName, SYSDATETIME() MessageDate, '                            Resolution : ' +  convert(VARCHAR,@Resolution)  AS MessageText;

		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
				SELECT @DebugId SessionId, 'GenerateReport' ApplicationName, SYSDATETIME() MessageDate, '                            Resolution : Hour = 0, FifteenMinute = 1, ThirtyMinute = 2, FiveMinute = 3, Minute = 4, Day = 5, Month = 6, Year=7' As MessageText;

		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
				SELECT @DebugId SessionId, 'GenerateReport' ApplicationName, SYSDATETIME() MessageDate, '                            OffsetTotals : ' +  convert(VARCHAR,@OffsetTotalsTo)  AS MessageText;

		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
			SELECT @DebugId SessionId, 'GenerateReport' ApplicationName, SYSDATETIME() MessageDate, '         Filter list count = ' + convert( nvarchar(10),count(*))   AS MessageText  
			  FROM @FilterList;

		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
			SELECT @DebugId SessionId, 'GenerateReport' ApplicationName, SYSDATETIME() MessageDate, '                                    Filter: '+ cast(Filter as varchar(360)) AS MessageText  
			  FROM @FilterList;

		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
			SELECT @DebugId SessionId, 'GenerateReport' ApplicationName, SYSDATETIME() MessageDate, '         TypeName count = ' + convert( nvarchar(10),count(*))   AS MessageText  
			  FROM @TypeNameList;

		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
			SELECT @DebugId SessionId, 'GenerateReport' ApplicationName, SYSDATETIME() MessageDate, '                                  Typename: ' + TypeName  AS MessageText  
			  FROM @TypeNameList;

		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
			SELECT @DebugId SessionId, 'GenerateReport' ApplicationName, SYSDATETIME() MessageDate, '         Exclude Filter list count = ' + convert( nvarchar(10),count(*))   AS MessageText  
			  FROM @ExcludeFilterList;

		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
			SELECT @DebugId SessionId, 'GenerateReport' ApplicationName, SYSDATETIME() MessageDate, '                             ExcludeFilter: '+ cast(Filter as varchar(360)) AS MessageText  
			  FROM @ExcludeFilterList;

	END;

	DECLARE @Statement NVARCHAR(4000)
	DECLARE @StatementRemainder NVARCHAR(4000)
	DECLARE @OffsetColumnName NVARCHAR(200)
	DECLARE @Columns NVARCHAR(1000)
	DECLARE @ResolutionString NVARCHAR(400)
	DECLARE @ResolutionWithAlias NVARCHAR(400)
	DECLARE @TrackerFilterJoin NVARCHAR(1000)
	DECLARE @FilterListCondition NVARCHAR(1000)
	DECLARE @ExcludeFilterCondition NVARCHAR(1000)
	DECLARE @Where  NVARCHAR(2000)
	DECLARE @Group  NVARCHAR(1000)
	DECLARE @OrderBy NVARCHAR(1000)
	
	SET @Columns = 'sum(td.measurementValue) as MeasurementValue, td.MeasurementName, td.TypeName, td.KeyFilter as Filter, @FilterColumn @GroupedTime'
	SET @OrderBy = 'WITH ROLLUP ORDER BY @ResolutionString, TD.TypeName, @FilterGroup td.MeasurementName'
	
	SET @Statement = 'SELECT @@Columns 
		FROM TrackerData_Vw td WITH (NOLOCK) 
		@@TrackerFilterJoin
		WHERE @@Where 
		GROUP BY @@Group 
		@@OrderBy'
	
	SET @Where = 'td.TypeName IN (@Types) AND TD.MeasureDateTime BETWEEN @StartDt AND @EndDt'
	SET @Group = 'td.TypeName, @FilterGroup td.KeyFilter, td.MeasurementName, @ResolutionString'
	SET @TrackerFilterJoin = 'JOIN DBO.FilterGroupFilters fgf WITH (NOLOCK) ON @@FilterListCondition @@AddAnd @@ExcludeFilterList'
	set @FilterListCondition = ' td.FilterGroupId = fgf.FilterGroupId 
	JOIN DBO.SessionFilterLists sfl WITH (NOLOCK) ON SFL.FilterValue = fgf.FilterGroupFilterString AND SFL.SessionID = ''@@SessionId''
	AND SFL.FilterType = 0 ' 
	SET @ExcludeFilterCondition = ' fgf.filtergroupid in (  SELECT DISTINCT filtergroupid 
								FROM DBO.FilterGroupFilters fgf WITH (NOLOCK) 
								JOIN DBO.SessionFilterLists sfl WITH (NOLOCK) 
								  ON ISNULL(SFL.FilterValue,'''') = ISNULL(fgf.FilterGroupFilterString,'''')
								 AND SFL.SessionID = ''@@SessionId''
								 AND SFL.FilterType = 0
					   EXCEPT SELECT DISTINCT filtergroupid FROM FiltergroupFilters fgf2 
								JOIN SessionFilterLists sfl2
								  ON FGF2.FilterGroupFilterString = sfl2.FilterValue
								 AND sfl2.SessionId = ''@@SessionId'' 
								 AND sfl2.FilterType = 1)'


	/*   AND fgf.filtergroupid in (  SELECT DISTINCT filtergroupid 
								FROM DBO.FilterGroupFilters fgf WITH (NOLOCK) 
								JOIN DBO.SessionFilterLists sfl WITH (NOLOCK) 
								  ON ISNULL(SFL.FilterValue,'') = ISNULL(fgf.FilterGroupFilterString,'')
								 AND SFL.SessionID = 'F437602B-094E-46FC-A04B-9ADFDA9502FC'
								 AND SFL.FilterType = 0
					   EXCEPT SELECT DISTINCT filtergroupid FROM FiltergroupFilters fgf2 
								JOIN SessionFilterLists sfl2
								  ON FGF2.FilterGroupFilterString = sfl2.FilterValue
								 AND sfl2.SessionId = 'F437602B-094E-46FC-A04B-9ADFDA9502FC' 
								 AND sfl2.FilterType = 1)
	*/
	
--public enum Resolution 
--   { 
--       Hour = 0, 
--       FifteenMinute = 1, 
--       ThirtyMinute = 2, 
--       FiveMinute = 3, 
--       Minute = 4, 
--       Day = 5, 
--       Month = 6, 
--   }
	SET @ResolutionString = 
		CASE WHEN @Resolution=0
			 THEN 'CAST(CONVERT(VARCHAR(13),@OffsetColumnName, 120) + '':00:00'' as DateTime)'
			 WHEN @Resolution=1
			 THEN 'CASE WHEN DATEPART(MINUTE,@OffsetColumnName) = 0 THEN @OffsetColumnName
			       ELSE DATEADD(mi,(15- ((DATEPART(MINUTE,@OffsetColumnName) - ( 15 *(DATEPART(MINUTE,DATEADD(mi,-1,@OffsetColumnName))/ 15))))),@OffsetColumnName) 
			        END'
			 WHEN @Resolution=2
			 THEN 'CASE WHEN DATEPART(minute,@OffsetColumnName) = 0 THEN @OffsetColumnName
				   ELSE DATEADD(mi,(30- ((DATEPART(minute,@OffsetColumnName) - ( 30 *(DATEPART(minute,DATEADD(mi,-1,@OffsetColumnName))/ 30))))),@OffsetColumnName) 
				    END'
			 WHEN @Resolution=3
			 THEN 'CASE WHEN DATEPART(MINUTE,@OffsetColumnName) = 0 THEN @OffsetColumnName
				   ELSE DATEADD(mi,(5- ((DATEPART(MINUTE,@OffsetColumnName) - ( 5 *(DATEPART(MINUTE,DATEADD(mi,-1,@OffsetColumnName))/ 5))))),@OffsetColumnName) 
					END'
			 WHEN @Resolution=4
		  	 THEN '@OffsetColumnName'
			 WHEN @Resolution=5
		 	 THEN 'CAST(CONVERT(VARCHAR(10),@OffsetColumnName, 120) as DateTime)'
			 WHEN @Resolution=6
		     THEN 'CAST(CONVERT(VARCHAR(7),@OffsetColumnName, 120) + ''-01''  AS DateTime)'
			 WHEN @Resolution=7
		 	 THEN 'CAST(CONVERT(VARCHAR(4),@OffsetColumnName, 120) AS DateTime)'
			 ELSE 'CAST(CONVERT(VARCHAR(10),@OffsetColumnName, 120) as DateTime)'
		END
	
	SET @ResolutionWithAlias = @ResolutionString + ' AS MeasureTime'

	SET @Columns = REPLACE(@Columns, '@GroupedTime', '@ResolutionWithAlias')

	SET @OffsetColumnName = 
		   CASE WHEN @OffsetTotalsTo = -1	THEN 'MeasureDateTimeOffsetMinus1Hour'
				WHEN @OffsetTotalsTo = -2	THEN 'MeasureDateTimeOffsetMinus2Hour'
				WHEN @OffsetTotalsTo = -3	THEN 'MeasureDateTimeOffsetMinus3Hour'
				WHEN @OffsetTotalsTo = -4	THEN 'MeasureDateTimeOffsetMinus4Hour'
				WHEN @OffsetTotalsTo = -5	THEN 'MeasureDateTimeOffsetMinus5Hour'
				WHEN @OffsetTotalsTo = -6	THEN 'MeasureDateTimeOffsetMinus6Hour'
				WHEN @OffsetTotalsTo = -7	THEN 'MeasureDateTimeOffsetMinus7Hour'
				WHEN @OffsetTotalsTo = -8	THEN 'MeasureDateTimeOffsetMinus8Hour'
				WHEN @OffsetTotalsTo = -9	THEN 'MeasureDateTimeOffsetMinus9Hour'
				WHEN @OffsetTotalsTo = -10	THEN 'MeasureDateTimeOffsetMinus10Hour'
				WHEN @OffsetTotalsTo = -11	THEN 'MeasureDateTimeOffsetMinus11Hour'
				WHEN @OffsetTotalsTo = -12	THEN 'MeasureDateTimeOffsetMinus12Hour'
				WHEN @OffsetTotalsTo = 0	THEN 'MeasureDateTime'
				WHEN @OffsetTotalsTo = 1	THEN 'MeasureDateTimeOffsetPlus1Hour'
				WHEN @OffsetTotalsTo = 2	THEN 'MeasureDateTimeOffsetPlus2Hour'
				WHEN @OffsetTotalsTo = 3	THEN 'MeasureDateTimeOffsetPlus3Hour'
				WHEN @OffsetTotalsTo = 4	THEN 'MeasureDateTimeOffsetPlus4Hour'
				WHEN @OffsetTotalsTo = 5	THEN 'MeasureDateTimeOffsetPlus5Hour'
				WHEN @OffsetTotalsTo = 6	THEN 'MeasureDateTimeOffsetPlus6Hour'
				WHEN @OffsetTotalsTo = 7	THEN 'MeasureDateTimeOffsetPlus7Hour'
				WHEN @OffsetTotalsTo = 8	THEN 'MeasureDateTimeOffsetPlus8Hour'
				WHEN @OffsetTotalsTo = 9	THEN 'MeasureDateTimeOffsetPlus9Hour'
				WHEN @OffsetTotalsTo = 10	THEN 'MeasureDateTimeOffsetPlus10Hour'
				WHEN @OffsetTotalsTo = 11	THEN 'MeasureDateTimeOffsetPlus11Hour'
				WHEN @OffsetTotalsTo = 12	THEN 'MeasureDateTimeOffsetPlus12Hour'
				WHEN @OffsetTotalsTo = 13	THEN 'MeasureDateTimeOffsetPlus13Hour'
				WHEN @OffsetTotalsTo = 14	THEN 'MeasureDateTimeOffsetPlus14Hour'
			END 
	DECLARE @TypeNameListCopy TABLE 
	(
		TypeName NVARCHAR(100)
	)	
	DECLARE @TypeNameListString NVARCHAR(1000)
				
	INSERT INTO @TypeNameListCopy
	SELECT * FROM @TypeNameList



	IF (@SessionId IS NULL)
	BEGIN
		select @SessionId = newid();
		set @NewSession = 1;
		delete from SessionFilterLists where SessionId = @SessionId;
	END
	ELSE
		set @NewSession = 0;

	set @sessionExpiryDate = DATEADD(MINUTE,10,SYSDATETIME());

	if (@NewSession = 0) 
		update SessionFilterLists set SessionExpiryDate = @SessionExpiryDate WHERE SessionID = @SessionId;

	IF EXISTS (select * from @FilterList)
	BEGIN 
		IF (@DebugMode = 1)
		BEGIN
			INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
				SELECT @DebugId SessionId, 'GenerateReport' ApplicationName, SYSDATETIME() MessageDate, '        Doing include filter upsert. ' AS MessageText;
			SELECT @SessionRecordCount = COUNT(*) FROM @FilterList
			INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
				SELECT @DebugId SessionId, 'GenerateReport' ApplicationName, SYSDATETIME() MessageDate, '        FilterList contains ' +  CONVERT(VARCHAR,@SessionRecordCount) + ' filters' AS MessageText;
		END

		MERGE INTO SessionFilterLists AS SFL
		USING (SELECT DISTINCT @SessionId AS SessionID, Filter, @SessionExpiryDate AS SessionExpiryDate from @FilterList) PassedFL
		ON PassedFL.SessionId = SFL.SessionID
		AND PassedFL.Filter = SFL.FilterValue
		WHEN NOT MATCHED BY TARGET THEN
			INSERT (SessionId, FilterType, FilterValue, SessionExpiryDate)
			VALUES (@SessionId,0, PassedFL.Filter,PassedFL.SessionExpiryDate)
		WHEN MATCHED THEN
			UPDATE SET SFL.SessionExpiryDate = PassedFL.SessionExpiryDate,
						SFL.FilterType = 0;

		SET @TrackerFilterJoin = REPLACE(@TrackerFilterJoin,'@@FilterListCondition',@FilterListCondition)
	END
	ELSE
		SET @TrackerFilterJoin = REPLACE(@TrackerFilterJoin,'@@FilterListCondition','')

	IF (@DebugMode = 1)
	BEGIN
		SELECT @SessionRecordCount = COUNT(*) FROM SessionFilterLists WHERE SessionId = @SessionId AND filterType = 0 
		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
				SELECT @DebugId SessionId, 'GenerateReport' ApplicationName, SYSDATETIME() MessageDate, '        After upsert number of include filters = ' +  CONVERT(VARCHAR,@SessionRecordCount) AS MessageText;
		SELECT @SessionRecordCount = COUNT(*) FROM SessionFilterLists WHERE SessionId = @SessionId 
		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
				SELECT @DebugId SessionId, 'GenerateReport' ApplicationName, SYSDATETIME() MessageDate, '        After upsert number of filters = ' +  CONVERT(VARCHAR,@SessionRecordCount) AS MessageText;
	END


    IF EXISTS (select * from @ExcludeFilterList)
	BEGIN
		MERGE INTO SessionFilterLists AS SFL
		USING (SELECT DISTINCT @SessionId AS SessionID, Filter, @SessionExpiryDate AS SessionExpiryDate from @ExcludeFilterList) PassedFL
		ON PassedFL.SessionId = SFL.SessionID
		AND PassedFL.Filter = SFL.FilterValue
		WHEN NOT MATCHED BY TARGET THEN
			INSERT (SessionId, FilterType, FilterValue, SessionExpiryDate)
			VALUES (PassedFL.SessionId,1, PassedFL.Filter,PassedFL.SessionExpiryDate)
		WHEN MATCHED THEN
			UPDATE SET SFL.SessionExpiryDate = PassedFL.SessionExpiryDate,
						sfl.FilterType = 1;
		SET @TrackerFilterJoin = REPLACE(@TrackerFilterJoin,'@@ExcludeFilterList',@ExcludeFilterCondition)
	END
	ELSE
		SET @TrackerFilterJoin = REPLACE(@TrackerFilterJoin,'@@ExcludeFilterList','')
	
	IF (@DebugMode = 1)
	BEGIN
		SELECT @SessionRecordCount = COUNT(*) FROM SessionFilterLists WHERE SessionId = @SessionId AND filterType = 1 
		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
				SELECT @DebugId SessionId, 'GenerateReport' ApplicationName, SYSDATETIME() MessageDate, '        After upsert number of EXCLUDE filters = ' +  CONVERT(VARCHAR,@SessionRecordCount) AS MessageText;
		SELECT @SessionRecordCount = COUNT(*) FROM SessionFilterLists WHERE SessionId = @SessionId AND filterType = 0 
		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
				SELECT @DebugId SessionId, 'GenerateReport' ApplicationName, SYSDATETIME() MessageDate, '        After upsert number of include filters = ' +  CONVERT(VARCHAR,@SessionRecordCount) AS MessageText;
	END

	IF EXISTS (select * from @FilterList) AND EXISTS (select * from @ExcludeFilterList)
	BEGIN 
		SET @TrackerFilterJoin = REPLACE(@TrackerFilterJoin,'@@AddAnd','AND')
	END
	ELSE
		SET @TrackerFilterJoin = REPLACE(@TrackerFilterJoin,'@@AddAnd','')


	--select * from SessionFilterLists where SessionId = @SessionId

	SELECT @SessionRecordCount = COUNT(*) FROM SessionFilterLists WHERE SessionId = @SessionId

	IF (@SessionRecordCount > 0)
	BEGIN
		SET @TrackerFilterJoin = REPLACE(@TrackerFilterJoin,'@@SessionId',@SessionId)
		SET @Columns =  REPLACE(@Columns, '@FilterColumn', 'fgf.FilterGroupFilterString,')
		SET @Group =  REPLACE(@Group, '@FilterGroup', 'fgf.FilterGroupFilterString,')
		SET @OrderBy =  REPLACE(@OrderBy, '@FilterGroup', 'fgf.FilterGroupFilterString,')
	END
	ELSE
	BEGIN
		SET @Columns =  REPLACE(@Columns, '@FilterColumn', '')
		SET @TrackerFilterJoin = ''		
		SET @Group =  REPLACE(@Group, '@FilterGroup', '')
		SET @OrderBy =  REPLACE(@OrderBy, '@FilterGroup', '')
	END

	SET @TypeNameListString = ''
	WHILE EXISTS(SELECT * FROM @TypeNameListCopy)
	BEGIN
		DECLARE @Next NVARCHAR(100)
		SELECT TOP 1 @Next = TypeName FROM @TypeNameListCopy

		IF(@TypeNameListString = '')
			SET @TypeNameListString = '''' +  @Next + ''''
		ELSE
			SET @TypeNameListString = @TypeNameListString + ',' + '''' +  @Next + ''''

		DELETE FROM @TypeNameListCopy WHERE TypeName = @Next			
	END
	
	IF (@DebugMode = 1)
	BEGIN
		SELECT @SessionRecordCount = COUNT(*) FROM SessionFilterLists WHERE SessionId = @SessionId AND filterType = 1 
		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
				SELECT @DebugId SessionId, 'GenerateReport' ApplicationName, SYSDATETIME() MessageDate, '        After upsert number of EXCLUDE filters = ' +  CONVERT(VARCHAR,@SessionRecordCount) AS MessageText;
		SELECT @SessionRecordCount = COUNT(*) FROM SessionFilterLists WHERE SessionId = @SessionId AND filterType = 0 
		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
				SELECT @DebugId SessionId, 'GenerateReport' ApplicationName, SYSDATETIME() MessageDate, '        After upsert number of include filters = ' +  CONVERT(VARCHAR,@SessionRecordCount) AS MessageText;
	END

	SET @Where = REPLACE(@Where,'@Types',@TypeNameListString)	

	SET @Statement = REPLACE(@Statement, '@@Columns', @Columns)
	SET @Statement = REPLACE(@Statement,'@@Where',@Where)
	SET @Statement = REPLACE(@Statement,'@@Group', @Group)
	SET @Statement = REPLACE(@Statement,'@@TrackerFilterJoin', @TrackerFilterJoin)
	SET @Statement = REPLACE(@Statement, '@@Columns', @Columns)
	SET @Statement = REPLACE(@Statement,'@@Where',@Where)
	SET @Statement = REPLACE(@Statement, '@@OrderBy', @OrderBy)
	SET @Statement = REPLACE(@Statement, '@ResolutionString', @ResolutionString)
	SET @Statement = REPLACE(@Statement, '@ResolutionWithAlias', @ResolutionWithAlias)
	SET @Statement = REPLACE(@Statement, '@OffsetColumnName', @OffsetColumnName)
		
	DECLARE @Param nvarchar(500);
	SET @Param = N'@StartDt DATETIME, @EndDt DATETIME';
	
	IF (@DebugMode = 1)
	BEGIN
		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
				SELECT @DebugId SessionId, 'GenerateReport' ApplicationName, SYSDATETIME() MessageDate, 'Statement generated' AS MessageText;

		set @StatementRemainder = @Statement;
		WHILE (Len(@StatementRemainder) > 0)
		BEGIN
		    INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
				SELECT @DebugId SessionId, 'GenerateReport' ApplicationName, SYSDATETIME() MessageDate, SUBSTRING(@StatementRemainder,0,499) AS MessageText;
			set @StatementRemainder = SUBSTRING(@StatementRemainder,499,len(@StatementRemainder));
		END

	END
	PRINT @Statement
	EXECUTE SP_EXECUTESQL @Statement, @Param, @StartDt = @StartDt, @EndDt = @EndDt

	SELECT 
		@error = @@ERROR

	IF @error <> 0 
	BEGIN
		GOTO ERR_EXIT
	END

	OK_EXIT: 
		IF @return IS NULL SELECT @return = @RET_OK
		SET NOCOUNT OFF
		RETURN @return
	
	ERR_EXIT:
		IF (@return IS NULL OR @return = 0) SELECT @return = @error
		RETURN @return
END







GO
/****** Object:  StoredProcedure [dbo].[GenerateReportUsingBuckets]    Script Date: 9/15/2015 10:33:20 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE PROCEDURE [dbo].[GenerateReportUsingBuckets] 
    @StartDt DATETIME,
	@EndDt DATETIME,
	@Resolution INT,
	@FilterList AS dbo.FilterList READONLY,
	@TypeNameList AS dbo.TypeName READONLY,
	@ExcludeFilterList AS dbo.FilterList READONLY,
	@SessionId uniqueidentifier = NULL
AS 
BEGIN
	
	SET NOCOUNT ON

	DECLARE 
		@error					int,
		@return					int,
		@RET_OK					int,
		@NewSession				int,
		@SessionRecordCount		int,
		@DebugMode				int,
		@SessionExpiryDate		DateTime
	
	-- Set Return Values 
	SELECT 
		@RET_OK = 0

	DECLARE @Statement NVARCHAR(4000)
	DECLARE @SeedStatement NVARCHAR(4000)
	DECLARE @ViewName NVARCHAR(40)
	DECLARE @Columns NVARCHAR(1000)
	DECLARE @TrackerFilterJoin NVARCHAR(1000)
	DECLARE @ExcludeFilterCondition NVARCHAR(1000)
	DECLARE @Where  NVARCHAR(2000)
	DECLARE @Group  NVARCHAR(1000)
	DECLARE @OrderBy NVARCHAR(1000)
	DECLARE @DebugSessionId UniqueIdentifier	
	
	SET @DebugSessionId = newid();

	SET @Columns = 'sum(td.measurementValue) as MeasurementValue,td.MeasurementName,td.TypeName, @FilterColumn @GroupedTime'
	SET @SeedStatement = 'select @@Columns 
	FROM @@ViewName td WITH (NOLOCK)
    @@TrackerFilterJoin
   WHERE @@where
GROUP BY @@Group'

	SET @OrderBy = 'WITH ROLLUP ORDER BY TD.MeasureDateTime, TD.TypeName, @FilterGroup td.MeasurementName'
			

	SET @Statement = 'SELECT @@Columns FROM @@ViewName td WITH (NOLOCK) @@TrackerFilterJoin
		WHERE @@Where GROUP BY @@Group @@OrderBy'
		
	
	SET @Where = 'td.TypeName IN (@Types) AND td.MeasureDateTime BETWEEN @StartDt AND @EndDt'
	SET @Group = 'td.TypeName, @FilterGroup td.MeasurementName, TD.MeasureDateTime'
	SET @TrackerFilterJoin = 'JOIN DBO.FilterGroupFilters fgf WITH (NOLOCK) ON td.FilterGroupId = fgf.FilterGroupId 
	JOIN DBO.SessionFilterLists sfl WITH (NOLOCK) ON SFL.FilterValue = fgf.FilterGroupFilterString AND SFL.SessionID = ''@@SessionId''
	AND SFL.FilterType = 0 @@ExcludeFilterList'
	SET @ExcludeFilterCondition = 'AND FGF.FilterGroupId NOT IN (SELECT FGF.FiltergroupId FROM FiltergroupFilters fgf WHERE
	FGF.FilterGroupFilterString IN (SELECT FilterValue FROM SessionFilterLists WHERE SessionId = ''@@SessionId'' AND
	FilterType = 1))'

	SET @Columns = REPLACE(@Columns, '@GroupedTime', 'td.MeasureDateTime')

--public enum Resolution 
--   { 
--       Hour = 0, 
--       FifteenMinute = 1, 
--       ThirtyMinute = 2, 
--       FiveMinute = 3, 
--       Minute = 4, 
--       Day = 5, 
--       Month = 6, 
--   }
	--IF(@Resolution=0)
	--BEGIN
	--	SET @ViewName = 'TrackerDataHourly_Vw'
	--END	
	--ELSE IF(@Resolution=1)
	--BEGIN
	--	SET @ViewName = 'TrackerDataQuarterHourly_Vw'
	--END
	--ELSE IF(@Resolution=2)
	--BEGIN
	--	SET @ViewName = 'TrackerDataHalfHourly_Vw'
	--END
	--ELSE IF(@Resolution=3)
	--BEGIN
	--	SET @ViewName = 'TrackerDataFiveMinute_Vw'
	--END
	--ELSE IF(@Resolution=4)
	--BEGIN
	--	SET @ViewName = 'TrackerDataOneMinute_Vw'
	--END
	--ELSE IF(@Resolution=5)
	--BEGIN
	--	SET @ViewName = 'TrackerDataDaily_Vw'
	--END
	--ELSE IF(@Resolution=6)
	--BEGIN
	--	SET @ViewName = 'TrackerDataMonthly_Vw'
	--END	
	--ELSE IF(@Resolution=7)
	--BEGIN
	--	SET @ViewName = 'TrackerDataYearly_Vw'
	--END	
	--ELSE
	--BEGIN
	--	SET @ViewName = 'TrackerDataDaily_Vw'
	--END
	SET @ViewName = 'TrackerData_Vw'

	DECLARE @TypeNameListCopy TABLE 
	(
		TypeName NVARCHAR(100)
	)	
	DECLARE @TypeNameListString NVARCHAR(1000)
				
	INSERT INTO @TypeNameListCopy
	SELECT * FROM @TypeNameList



	IF (@SessionId IS NULL)
	BEGIN
		select @SessionId = newid();
		set @NewSession = 1;
		delete from SessionFilterLists where SessionId = @SessionId;
	END
	ELSE
		set @NewSession = 0;

	set @sessionExpiryDate = DATEADD(MINUTE,10,SYSDATETIME());

	if (@NewSession = 0) 
		update SessionFilterLists set SessionExpiryDate = @SessionExpiryDate WHERE SessionID = @SessionId;
	--select * from @FilterList;

	MERGE INTO SessionFilterLists AS SFL
	USING (Select @SessionId AS SessionID, Filter, @SessionExpiryDate AS SessionExpiryDate from @FilterList) PassedFL
	ON PassedFL.SessionId = SFL.SessionID
	AND PassedFL.Filter = SFL.FilterValue
	WHEN NOT MATCHED BY TARGET THEN
		INSERT (SessionId, FilterType, FilterValue, SessionExpiryDate)
		VALUES (PassedFL.SessionId,0, PassedFL.Filter,PassedFL.SessionExpiryDate)
	WHEN MATCHED THEN
		UPDATE SET SFL.SessionExpiryDate = PassedFL.SessionExpiryDate,
					SFL.FilterType = 0;

    IF EXISTS (select * from @ExcludeFilterList)
	BEGIN
		MERGE INTO SessionFilterLists AS SFL
		USING (Select @SessionId AS SessionID, Filter, @SessionExpiryDate AS SessionExpiryDate from @ExcludeFilterList) PassedFL
		ON PassedFL.SessionId = SFL.SessionID
		AND PassedFL.Filter = SFL.FilterValue
		WHEN NOT MATCHED BY TARGET THEN
			INSERT (SessionId, FilterType, FilterValue, SessionExpiryDate)
			VALUES (PassedFL.SessionId,1, PassedFL.Filter,PassedFL.SessionExpiryDate)
		WHEN MATCHED THEN
			UPDATE SET SFL.SessionExpiryDate = PassedFL.SessionExpiryDate,
						sfl.FilterType = 1;
		SET @TrackerFilterJoin = REPLACE(@TrackerFilterJoin,'@@ExcludeFilterList',@ExcludeFilterCondition)
	END
	ELSE
		SET @TrackerFilterJoin = REPLACE(@TrackerFilterJoin,'@@ExcludeFilterList','')
		
	--select * from SessionFilterLists where SessionId = @SessionId

	SELECT @SessionRecordCount = COUNT(*) FROM SessionFilterLists WHERE SessionId = @SessionId

	IF (@SessionRecordCount > 0)
	BEGIN
		SET @TrackerFilterJoin = REPLACE(@TrackerFilterJoin,'@@SessionId',@SessionId)
		SET @Columns =  REPLACE(@Columns, '@FilterColumn', 'fgf.FilterGroupFilterString,')
		SET @Group =  REPLACE(@Group, '@FilterGroup', 'fgf.FilterGroupFilterString,')
		SET @OrderBy =  REPLACE(@OrderBy, '@FilterGroup', 'fgf.FilterGroupFilterString,')
	END
	ELSE
	BEGIN
		SET @Columns =  REPLACE(@Columns, '@FilterColumn', '')
		SET @TrackerFilterJoin = ''
		SET @Where = 'td.TypeName IN (@Types) AND td.MeasureDateTime BETWEEN @StartDt AND @EndDt'
		SET @Group =  REPLACE(@Group, '@FilterGroup', '')
		SET @OrderBy =  REPLACE(@OrderBy, '@FilterGroup', '')
	END

	SET @TypeNameListString = ''
	WHILE EXISTS(SELECT * FROM @TypeNameListCopy)
	BEGIN
		DECLARE @Next NVARCHAR(100)
		SELECT TOP 1 @Next = TypeName FROM @TypeNameListCopy

		IF(@TypeNameListString = '')
			SET @TypeNameListString = '''' +  @Next + ''''
		ELSE
			SET @TypeNameListString = @TypeNameListString + ',' + '''' +  @Next + ''''

		DELETE FROM @TypeNameListCopy WHERE TypeName = @Next			
	END
	
	SET @Where = REPLACE(@Where,'@Types',@TypeNameListString)	

	SET @Statement = REPLACE(@Statement, '@@ViewName', @ViewName)
	SET @Statement = REPLACE(@Statement, '@@Columns', @Columns)
	SET @Statement = REPLACE(@Statement,'@@Where',@Where)
	SET @Statement = REPLACE(@Statement,'@@Group', @Group)
	SET @Statement = REPLACE(@Statement,'@@TrackerFilterJoin', @TrackerFilterJoin)
	SET @Statement = REPLACE(@Statement, '@@Columns', @Columns)
	SET @Statement = REPLACE(@Statement,'@@Where',@Where)
	SET @Statement = REPLACE(@Statement, '@@OrderBy', @OrderBy)

	INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
			SELECT @DebugSessionId SessionId, 'GenerateReportUsingBuckets' ApplicationName, SYSDATETIME() MessageDate, 'Generate report called with Start Date: ' +  convert(VARCHAR,@StartDt) + ' End Date: ' + convert(VARCHAR,@EndDt) + ' Resolution: ' + cast(@Resolution as varchar(10)) + ' SessionId: ' + cast(@SessionId as varchar(64))  AS MessageText;

	INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
			SELECT @DebugSessionId SessionId, 'GenerateReportUsingBuckets' ApplicationName, SYSDATETIME() MessageDate, 'Filter: '+ cast(Filter as varchar(360)) AS MessageText
			FROM @FilterList;

	INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
			SELECT @DebugSessionId SessionId, 'GenerateReportUsingBuckets' ApplicationName, SYSDATETIME() MessageDate, 'Exclude Filter: '+ cast(Filter as varchar(360)) AS MessageText
			FROM @ExcludeFilterList;


	DECLARE @Param nvarchar(500);
	SET @Param = N'@StartDt DATETIME, @EndDt DATETIME';
	PRINT @Statement
	EXECUTE SP_EXECUTESQL @Statement, @Param, @StartDt = @StartDt, @EndDt = @EndDt

	SELECT 
		@error = @@ERROR

	IF @error <> 0 
	BEGIN
		GOTO ERR_EXIT
	END

	OK_EXIT: 
		IF @return IS NULL SELECT @return = @RET_OK
		SET NOCOUNT OFF
		RETURN @return
	
	ERR_EXIT:
		IF (@return IS NULL OR @return = 0) SELECT @return = @error
		RETURN @return
END



GO
/****** Object:  StoredProcedure [dbo].[GenerateReportWithOffsetTotals]    Script Date: 9/15/2015 10:33:20 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


/****** Object:  StoredProcedure [dbo].[GenerateReport]    Script Date: 2/25/2015 10:34:34 AM ******/
--CREATE PROCEDURE [dbo].[GenerateReport] 
CREATE PROCEDURE [dbo].[GenerateReportWithOffsetTotals] 
    @StartDt DATETIME,
	@EndDt DATETIME,
	@Resolution INT,
	@FilterList AS dbo.FilterList READONLY,
	@TypeNameList AS dbo.TypeName READONLY,
	@ExcludeFilterList AS dbo.FilterList READONLY,
	@OffsetTotalsTo AS INT = 8,
	@SessionId uniqueidentifier = NULL
AS 
BEGIN
	
	SET NOCOUNT ON

	DECLARE 
		@error					int,
		@return					int,
		@RET_OK					int,
		@NewSession				int,
		@SessionRecordCount		int,
		@SessionExpiryDate		DateTime
	
	-- Set Return Values 
	SELECT 
		@RET_OK = 0

	DECLARE @Statement NVARCHAR(4000)
	DECLARE @OffsetColumnName NVARCHAR(200)
	DECLARE @Columns NVARCHAR(1000)
	DECLARE @ResolutionString NVARCHAR(400)
	DECLARE @ResolutionWithAlias NVARCHAR(400)
	DECLARE @TrackerFilterJoin NVARCHAR(1000)
	DECLARE @ExcludeFilterCondition NVARCHAR(1000)
	DECLARE @Where  NVARCHAR(2000)
	DECLARE @Group  NVARCHAR(1000)
	DECLARE @OrderBy NVARCHAR(1000)
	
	SET @Columns = 'sum(td.measurementValue) as MeasurementValue, td.MeasurementName, td.TypeName, @FilterColumn @GroupedTime'
	SET @OrderBy = 'WITH ROLLUP ORDER BY @ResolutionString, TD.TypeName, @FilterGroup td.MeasurementName'
	
	SET @Statement = 'SELECT @@Columns 
		FROM TrackerData_Vw td WITH (NOLOCK) 
		@@TrackerFilterJoin
		WHERE @@Where 
		GROUP BY @@Group 
		@@OrderBy'
	
	SET @Where = 'td.TypeName IN (@Types) AND TD.MeasureDateTime BETWEEN @StartDt AND @EndDt'
	SET @Group = 'td.TypeName, @FilterGroup td.MeasurementName, @ResolutionString'
	SET @TrackerFilterJoin = 'JOIN DBO.FilterGroupFilters fgf WITH (NOLOCK) ON td.FilterGroupId = fgf.FilterGroupId 
	JOIN DBO.SessionFilterLists sfl WITH (NOLOCK) ON SFL.FilterValue = fgf.FilterGroupFilterString AND SFL.SessionID = ''@@SessionId''
	AND SFL.FilterType = 0 @@ExcludeFilterList'
	SET @ExcludeFilterCondition = 'AND FGF.FilterGroupId NOT IN (SELECT FGF.FiltergroupId FROM FiltergroupFilters fgf WHERE
	FGF.FilterGroupFilterString IN (SELECT FilterValue FROM SessionFilterLists WHERE SessionId = ''@@SessionId'' AND
	FilterType = 1))'

	
--public enum Resolution 
--   { 
--       Hour = 0, 
--       FifteenMinute = 1, 
--       ThirtyMinute = 2, 
--       FiveMinute = 3, 
--       Minute = 4, 
--       Day = 5, 
--       Month = 6, 
--   }
	SET @ResolutionString = 
		CASE WHEN @Resolution=0
			 THEN 'CAST(CONVERT(VARCHAR(13),@OffsetColumnName, 120) + '':00:00'' as DateTime)'
			 WHEN @Resolution=1
			 THEN 'CASE WHEN DATEPART(MINUTE,@OffsetColumnName) = 0 THEN @OffsetColumnName
			       ELSE DATEADD(mi,(15- ((DATEPART(MINUTE,@OffsetColumnName) - ( 15 *(DATEPART(MINUTE,DATEADD(mi,-1,@OffsetColumnName))/ 15))))),@OffsetColumnName) 
			        END'
			 WHEN @Resolution=2
			 THEN 'CASE WHEN DATEPART(minute,@OffsetColumnName) = 0 THEN @OffsetColumnName
				   ELSE DATEADD(mi,(30- ((DATEPART(minute,@OffsetColumnName) - ( 30 *(DATEPART(minute,DATEADD(mi,-1,@OffsetColumnName))/ 30))))),@OffsetColumnName) 
				    END'
			 WHEN @Resolution=3
			 THEN 'CASE WHEN DATEPART(MINUTE,@OffsetColumnName) = 0 THEN @OffsetColumnName
				   ELSE DATEADD(mi,(5- ((DATEPART(MINUTE,@OffsetColumnName) - ( 5 *(DATEPART(MINUTE,DATEADD(mi,-1,@OffsetColumnName))/ 5))))),@OffsetColumnName) 
					END'
			 WHEN @Resolution=4
		  	 THEN '@OffsetColumnName'
			 WHEN @Resolution=5
		 	 THEN 'CAST(CONVERT(VARCHAR(10),@OffsetColumnName, 120) as DateTime)'
			 WHEN @Resolution=6
		     THEN 'CAST(CONVERT(VARCHAR(7),@OffsetColumnName, 120) + ''-01''  AS DateTime)'
			 WHEN @Resolution=7
		 	 THEN 'CAST(CONVERT(VARCHAR(4),@OffsetColumnName, 120) AS DateTime)'
			 ELSE 'CAST(CONVERT(VARCHAR(10),@OffsetColumnName, 120) as DateTime)'
		END
	
	SET @ResolutionWithAlias = @ResolutionString + ' AS MeasureTime'

	SET @Columns = REPLACE(@Columns, '@GroupedTime', '@ResolutionWithAlias')

	SET @OffsetColumnName = 
		   CASE WHEN @OffsetTotalsTo = -1	THEN 'MeasureDateTimeOffsetMinus1Hour'
				WHEN @OffsetTotalsTo = -2	THEN 'MeasureDateTimeOffsetMinus2Hour'
				WHEN @OffsetTotalsTo = -3	THEN 'MeasureDateTimeOffsetMinus3Hour'
				WHEN @OffsetTotalsTo = -4	THEN 'MeasureDateTimeOffsetMinus4Hour'
				WHEN @OffsetTotalsTo = -5	THEN 'MeasureDateTimeOffsetMinus5Hour'
				WHEN @OffsetTotalsTo = -6	THEN 'MeasureDateTimeOffsetMinus6Hour'
				WHEN @OffsetTotalsTo = -7	THEN 'MeasureDateTimeOffsetMinus7Hour'
				WHEN @OffsetTotalsTo = -8	THEN 'MeasureDateTimeOffsetMinus8Hour'
				WHEN @OffsetTotalsTo = -9	THEN 'MeasureDateTimeOffsetMinus9Hour'
				WHEN @OffsetTotalsTo = -10	THEN 'MeasureDateTimeOffsetMinus10Hour'
				WHEN @OffsetTotalsTo = -11	THEN 'MeasureDateTimeOffsetMinus11Hour'
				WHEN @OffsetTotalsTo = -12	THEN 'MeasureDateTimeOffsetMinus12Hour'
				WHEN @OffsetTotalsTo = 0	THEN 'MeasureDateTime'
				WHEN @OffsetTotalsTo = 1	THEN 'MeasureDateTimeOffsetPlus1Hour'
				WHEN @OffsetTotalsTo = 2	THEN 'MeasureDateTimeOffsetPlus2Hour'
				WHEN @OffsetTotalsTo = 3	THEN 'MeasureDateTimeOffsetPlus3Hour'
				WHEN @OffsetTotalsTo = 4	THEN 'MeasureDateTimeOffsetPlus4Hour'
				WHEN @OffsetTotalsTo = 5	THEN 'MeasureDateTimeOffsetPlus5Hour'
				WHEN @OffsetTotalsTo = 6	THEN 'MeasureDateTimeOffsetPlus6Hour'
				WHEN @OffsetTotalsTo = 7	THEN 'MeasureDateTimeOffsetPlus7Hour'
				WHEN @OffsetTotalsTo = 8	THEN 'MeasureDateTimeOffsetPlus8Hour'
				WHEN @OffsetTotalsTo = 9	THEN 'MeasureDateTimeOffsetPlus9Hour'
				WHEN @OffsetTotalsTo = 10	THEN 'MeasureDateTimeOffsetPlus10Hour'
				WHEN @OffsetTotalsTo = 11	THEN 'MeasureDateTimeOffsetPlus11Hour'
				WHEN @OffsetTotalsTo = 12	THEN 'MeasureDateTimeOffsetPlus12Hour'
				WHEN @OffsetTotalsTo = 13	THEN 'MeasureDateTimeOffsetPlus13Hour'
				WHEN @OffsetTotalsTo = 14	THEN 'MeasureDateTimeOffsetPlus14Hour'
			END 
	DECLARE @TypeNameListCopy TABLE 
	(
		TypeName NVARCHAR(100)
	)	
	DECLARE @TypeNameListString NVARCHAR(1000)
				
	INSERT INTO @TypeNameListCopy
	SELECT * FROM @TypeNameList



	IF (@SessionId IS NULL)
	BEGIN
		select @SessionId = newid();
		set @NewSession = 1;
		delete from SessionFilterLists where SessionId = @SessionId;
	END
	ELSE
		set @NewSession = 0;

	set @sessionExpiryDate = DATEADD(MINUTE,10,SYSDATETIME());

	if (@NewSession = 0) 
		update SessionFilterLists set SessionExpiryDate = @SessionExpiryDate WHERE SessionID = @SessionId;
	--select * from @FilterList;

	MERGE INTO SessionFilterLists AS SFL
	USING (Select @SessionId AS SessionID, Filter, @SessionExpiryDate AS SessionExpiryDate from @FilterList) PassedFL
	ON PassedFL.SessionId = SFL.SessionID
	AND PassedFL.Filter = SFL.FilterValue
	WHEN NOT MATCHED BY TARGET THEN
		INSERT (SessionId, FilterType, FilterValue, SessionExpiryDate)
		VALUES (PassedFL.SessionId,0, PassedFL.Filter,PassedFL.SessionExpiryDate)
	WHEN MATCHED THEN
		UPDATE SET SFL.SessionExpiryDate = PassedFL.SessionExpiryDate,
					SFL.FilterType = 0;

    IF EXISTS (select * from @ExcludeFilterList)
	BEGIN
		MERGE INTO SessionFilterLists AS SFL
		USING (Select @SessionId AS SessionID, Filter, @SessionExpiryDate AS SessionExpiryDate from @ExcludeFilterList) PassedFL
		ON PassedFL.SessionId = SFL.SessionID
		AND PassedFL.Filter = SFL.FilterValue
		WHEN NOT MATCHED BY TARGET THEN
			INSERT (SessionId, FilterType, FilterValue, SessionExpiryDate)
			VALUES (PassedFL.SessionId,1, PassedFL.Filter,PassedFL.SessionExpiryDate)
		WHEN MATCHED THEN
			UPDATE SET SFL.SessionExpiryDate = PassedFL.SessionExpiryDate,
						sfl.FilterType = 1;
		SET @TrackerFilterJoin = REPLACE(@TrackerFilterJoin,'@@ExcludeFilterList',@ExcludeFilterCondition)
	END
	ELSE
		SET @TrackerFilterJoin = REPLACE(@TrackerFilterJoin,'@@ExcludeFilterList','')
		
	--select * from SessionFilterLists where SessionId = @SessionId

	SELECT @SessionRecordCount = COUNT(*) FROM SessionFilterLists WHERE SessionId = @SessionId

	IF (@SessionRecordCount > 0)
	BEGIN
		SET @TrackerFilterJoin = REPLACE(@TrackerFilterJoin,'@@SessionId',@SessionId)
		SET @Columns =  REPLACE(@Columns, '@FilterColumn', 'fgf.FilterGroupFilterString,')
		SET @Group =  REPLACE(@Group, '@FilterGroup', 'fgf.FilterGroupFilterString,')
		SET @OrderBy =  REPLACE(@OrderBy, '@FilterGroup', 'fgf.FilterGroupFilterString,')
	END
	ELSE
	BEGIN
		SET @Columns =  REPLACE(@Columns, '@FilterColumn', '')
		SET @TrackerFilterJoin = ''
		SET @Where = 'td.TypeName IN (@Types) AND @OffsetColumnName BETWEEN @StartDt AND @EndDt'
		SET @Group =  REPLACE(@Group, '@FilterGroup', '')
		SET @OrderBy =  REPLACE(@OrderBy, '@FilterGroup', '')
	END

	SET @TypeNameListString = ''
	WHILE EXISTS(SELECT * FROM @TypeNameListCopy)
	BEGIN
		DECLARE @Next NVARCHAR(100)
		SELECT TOP 1 @Next = TypeName FROM @TypeNameListCopy

		IF(@TypeNameListString = '')
			SET @TypeNameListString = '''' +  @Next + ''''
		ELSE
			SET @TypeNameListString = @TypeNameListString + ',' + '''' +  @Next + ''''

		DELETE FROM @TypeNameListCopy WHERE TypeName = @Next			
	END
	
	SET @Where = REPLACE(@Where,'@Types',@TypeNameListString)	

	SET @Statement = REPLACE(@Statement, '@@Columns', @Columns)
	SET @Statement = REPLACE(@Statement,'@@Where',@Where)
	SET @Statement = REPLACE(@Statement,'@@Group', @Group)
	SET @Statement = REPLACE(@Statement,'@@TrackerFilterJoin', @TrackerFilterJoin)
	SET @Statement = REPLACE(@Statement, '@@Columns', @Columns)
	SET @Statement = REPLACE(@Statement,'@@Where',@Where)
	SET @Statement = REPLACE(@Statement, '@@OrderBy', @OrderBy)
	SET @Statement = REPLACE(@Statement, '@ResolutionString', @ResolutionString)
	SET @Statement = REPLACE(@Statement, '@ResolutionWithAlias', @ResolutionWithAlias)
	SET @Statement = REPLACE(@Statement, '@OffsetColumnName', @OffsetColumnName)
		
	DECLARE @Param nvarchar(500);
	SET @Param = N'@StartDt DATETIME, @EndDt DATETIME';
	PRINT @Statement
	EXECUTE SP_EXECUTESQL @Statement, @Param, @StartDt = @StartDt, @EndDt = @EndDt

	SELECT 
		@error = @@ERROR

	IF @error <> 0 
	BEGIN
		GOTO ERR_EXIT
	END

	OK_EXIT: 
		IF @return IS NULL SELECT @return = @RET_OK
		SET NOCOUNT OFF
		RETURN @return
	
	ERR_EXIT:
		IF (@return IS NULL OR @return = 0) SELECT @return = @error
		RETURN @return
END



GO
/****** Object:  StoredProcedure [dbo].[MigrateTrackers]    Script Date: 9/15/2015 10:33:20 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[MigrateTrackers] 
   AS 
BEGIN

	SET NOCOUNT ON
	
	DECLARE @resolutions TABLE (bigResolution INT, smallResolution INT)
	DECLARE @trackerTypesToDelete TABLE (TrackerTypeId	UNIQUEIDENTIFIER, TrackerResolution INT)

	DECLARE
		@error				INT,
		@return				INT,
		@RET_OK				INT,
		@DebugMode			INT,
		@biggerResolution	INT,
		@smallerResolution	INT,
		@UnMigratedCount    INT,
		@AllDocumentCount	INT,
		@lockStatus         VARCHAR(400),
		@IHaveTheLock       INT = 0,
		@UTCToLocalMidnightOffset int,
		@UTCTimeZoneMidnightOffset int,
		@DebugSessionId     UniqueIdentifier,
		@GenerateBuckets    INT = 0
	
	SET @DebugSessionId = newid();

	SELECT TOP 1 @lockStatus = messageText FROM ProcessingLog
	 WHERE MessageText LIKE 'Migration Active%' OR MessageText LIKE 'Migration Complete%'
	   AND ApplicationName = 'MigrateTrackers'
	ORDER BY messageDate DESC

	IF (@lockStatus LIKE 'Migration Active%')
	BEGIN
	  INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
			SELECT @DebugSessionId SessionId, 'MigrateTrackers' ApplicationName, SYSDATETIME() MessageDate, cast('Migration already running.  Exiting ' +  CONVERT(VARCHAR,SYSDATETIME()) as varchar(400)) AS MessageText;
	  GOTO OK_EXIT
	END


	INSERT INTO @resolutions (bigResolution,smallResolution)
		   (SELECT 43200,1440
				UNION
			SELECT 1440, 60
				UNION
			SELECT 60, 30
				UNION
			SELECT 30,15
				UNION 
			SELECT 15, 5
				UNION 
			SELECT 5,1	
			)

   	SET @RET_OK = 0
	SET @DebugMode = 1
	SET @UTCToLocalMidnightOffset = -8
	SET @UTCTimeZoneMidnightOffset = 8
	
	
	INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
		SELECT @DebugSessionId SessionId, 'MigrateTrackers' ApplicationName, SYSDATETIME() MessageDate, cast('Migration active ' +  CONVERT(VARCHAR,SYSDATETIME()) as varchar(400)) AS MessageText;
	set @IHaveTheLock = 1
	IF (@DebugMode = 1)
	BEGIN
		SELECT 'Locking Migration ' +  CONVERT(VARCHAR,SYSDATETIME()) AS MigrationLocked;
	END

	INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
		SELECT @DebugSessionId SessionId, 'MigrateTrackers' ApplicationName, SYSDATETIME() MessageDate, 'Extracting Tracker Types from xml documents started' AS MessageText ;
	IF (@DebugMode = 1)
	BEGIN
    	SELECT 'Extracting Tracker Types from xml documents started at ' +  CONVERT(VARCHAR,SYSDATETIME()) AS TrackerDefinitions;
	END

	BEGIN TRANSACTION
	MERGE INTO TrackerTypes AS TT
	USING (SELECT * 
			 FROM (SELECT DISTINCT  td.Tracker_Xml_Document.value('(/TrackerDataPoint/TypeName/text())[1]', 'nvarchar(300)') TypeName,
									td.Tracker_Xml_Document.value('(/TrackerDataPoint/Name/text())[1]', 'nvarchar(300)') TrackerTypeDesc
					 FROM [dbo].[TrackerDocuments] td
					WHERE TD.Document_Converted_By_Bulk_Load = 'N'
		 		  ) TrackerDetails
			WHERE TrackerDetails.TypeName IS NOT NULL
		  ) AS tfd
	   ON tt.TrackerTypeName = tfd.TypeName
	 WHEN NOT MATCHED BY TARGET THEN
	   	  INSERT (TrackerTypeName, TrackerTypeDescription, TrackerTypeMinimumResolution)
		  VALUES (tfd.TypeName, tfd.TrackerTypeDesc, 5);

	INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
		SELECT @DebugSessionId SessionId, 'MigrateTrackers' ApplicationName, SYSDATETIME() MessageDate, 'Merging Tracker Measures for Tracker Types' AS MessageText 
	IF (@DebugMode = 1)
	BEGIN
    	SELECT 'Merging Tracker Measures for Tracker Types at ' +  CONVERT(VARCHAR,SYSDATETIME()) AS TrackerMeasures;
	END

	MERGE INTO TrackerMeasures AS TM
	USING (SELECT tfd.TypeName, tfd.MeasurementName, TT.TrackerTypeId
	         FROM (SELECT DISTINCT td.Tracker_Xml_Document.value('(/TrackerDataPoint/TypeName/text())[1]', 'nvarchar(300)') TypeName,
									tdtn.tnav.value('declare namespace d2p1="http://schemas.microsoft.com/2003/10/Serialization/Arrays";
						   							(d2p1:Key/text())[1]', 'nvarchar(300)') MeasurementName
					 FROM TrackerDocuments td
			  CROSS APPLY td.Tracker_Xml_Document.nodes('declare namespace d2p1="http://schemas.microsoft.com/2003/10/Serialization/Arrays";
					   									 declare namespace i="http://www.w3.org/2001/XMLSchema-instance";
														 /TrackerDataPoint/Measurement/d2p1:KeyValueOfstringlong') AS tdtn(tnav)
					WHERE TD.Document_Converted_By_Bulk_Load = 'N'
				  ) AS tfd 
			 JOIN TrackerTypes TT
	  		   ON tt.TrackerTypeName = tfd.TypeName
		  ) TTMD
	   ON ttmd.TrackerTypeId = TM.TrackerTypeId
	  AND ttmd.MeasurementName = tm.TrackerMeasureName
	 WHEN NOT MATCHED BY TARGET THEN
	   	  INSERT (TrackerMeasureName, TrackerTypeId)
		  VALUES (ttmd.MeasurementName, ttmd.TrackerTypeId);

	INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
		SELECT @DebugSessionId SessionId, 'MigrateTrackers' ApplicationName, SYSDATETIME() MessageDate, 'Extracting Tracker definitions from xml documents started' AS MessageText  
	IF (@DebugMode = 1)
	BEGIN
    	SELECT 'Extracting Tracker definitions from xml documents started at ' +  CONVERT(VARCHAR,SYSDATETIME()) AS Trackers;
	END

	MERGE INTO Tracker AS T
	USING (SELECT trackerDetails.TrackerId, TT.TrackerTypeId
			 FROM (SELECT DISTINCT td.Tracker_Xml_Document.value('(/TrackerDataPoint/TypeName/text())[1]', 'nvarchar(300)') 
											+ ISNULL(td.Tracker_Xml_Document.value('(/TrackerDataPoint/KeyFilter/text())[1]', 'nvarchar(300)'),'') TrackerId,
									td.Tracker_Xml_Document.value('(/TrackerDataPoint/TypeName/text())[1]', 'nvarchar(300)') TypeName
					 FROM [dbo].[TrackerDocuments] td
					WHERE TD.Document_Converted_By_Bulk_Load = 'N'
		 		  ) TrackerDetails, TrackerTypes tt
			WHERE TrackerDetails.trackerId IS NOT NULL
			  AND TrackerDetails.TypeName = tt.TrackerTypeName
		  ) AS tfd
	   ON T.TrackerId = tfd.TrackerID
	  AND T.TrackerTypeId = tfd.TrackerTypeId
	 WHEN NOT MATCHED BY TARGET THEN
	   	  INSERT (TrackerId, TrackerTypeId)
		  VALUES (tfd.TrackerID, tfd.TrackerTypeId);

	COMMIT TRANSACTION

	INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
		SELECT @DebugSessionId SessionId, 'MigrateTrackers' ApplicationName, SYSDATETIME() MessageDate, 'Extracting Filter Group definitions from xml documents started' AS MessageText  
	IF (@DebugMode = 1)
	BEGIN
    	SELECT 'Extracting Filter Group definitions from xml documents started at ' +  CONVERT(VARCHAR,SYSDATETIME()) AS FilterGroups;
	END
	
	BEGIN TRANSACTION
	MERGE INTO FilterGroups AS FG
    USING (SELECT DISTINCT ISNULL(td.Tracker_Xml_Document.value('(/TrackerDataPoint/KeyFilter/text())[1]', 'nvarchar(300)'),null) KeyFilter
			  FROM TrackerDocuments td
			 WHERE TD.Document_Converted_By_Bulk_Load = 'N'
		  ) AS DFK
	   ON ISNULL(FG.FilterGroupKeyFilter,'') = ISNULL(DFK.KeyFilter,'')
	 WHEN NOT MATCHED BY TARGET THEN
		  INSERT (FilterGroupKeyFilter)
		  VALUES (ISNULL(KeyFilter,NULL));

	INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
		SELECT @DebugSessionId SessionId, 'MigrateTrackers' ApplicationName, SYSDATETIME() MessageDate, 'Extracting Filter Group Filter value combinations from xml documents started' AS MessageText  
	IF (@DebugMode = 1)
	BEGIN
    	SELECT 'Extracting Filter Group Filter value combinations from xml documents started at ' +  CONVERT(VARCHAR,SYSDATETIME()) AS FilterGroupFilterValues;
	END

	MERGE INTO FilterGroupFilters AS FGF
	USING (SELECT DISTINCT fg.FilterGroupID, ISNULL(fnav.KeyFilter,NULL) KeyFilter, ISNULL(fnav.filter_and_value,NULL) Filter_And_Value
			 FROM (SELECT ISNULL(td.Tracker_Xml_Document.value('(/TrackerDataPoint/KeyFilter/text())[1]', 'nvarchar(300)'),NULL) KeyFilter,
  			  	          ISNULL(tdsf.sf.value('(text())[1]', 'nvarchar(300)'),NULL) filter_and_value
			         FROM [dbo].[TrackerDocuments] td
  	          OUTER APPLY td.Tracker_Xml_Document.nodes('declare namespace d2p1="http://schemas.microsoft.com/2003/10/Serialization/Arrays";
		  										         declare namespace i="http://www.w3.org/2001/XMLSchema-instance";
													     /TrackerDataPoint/SearchFilters/d2p1:string') as tdsf(sf)
					WHERE TD.Document_Converted_By_Bulk_Load = 'N'
  				  ) fnav, 
				  FilterGroups fg
		    WHERE isnull(fnav.KeyFilter,'') = isnull(fg.FilterGroupKeyFilter,'')
		   ) AS DFGFV 
	   ON FGF.FilterGroupId = DFGFV.FilterGroupID
	  AND ISNULL(FGF.FilterGroupKeyFilter,'') = ISNULL(DFGFV.KeyFilter,'')
	  AND ISNULL(FGF.FilterGroupFilterString,'') = ISNULL(DFGFV.filter_and_value,'')
     WHEN NOT MATCHED BY TARGET THEN
		  INSERT  (FilterGroupId, FilterGroupKeyFilter, FilterGroupFilterString)
		  VALUES (DFGFV.FilterGroupId, ISNULL(DFGFV.KeyFilter,NULL), ISNULL(DFGFV.Filter_and_value,NULL));

	INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
		SELECT @DebugSessionId SessionId, 'MigrateTrackers' ApplicationName, SYSDATETIME() MessageDate, 'Adding missing KeyFilter combination to Filter Group Filter Values' AS MessageText  
	IF (@DebugMode = 1)
	BEGIN
    	SELECT 'Adding missing KeyFilter combination to Filter Group Filter Values at ' +  CONVERT(VARCHAR,SYSDATETIME()) AS FilterGroupFilterValuesBackfill;
	END
		
	MERGE INTO FilterGroupFilters fgf
	USING (SELECT FilterGroupId, FilterGroupKeyFilter, FilterGroupKeyFilter as FilterGroupFilterString
		     FROM FilterGroups
		  ) AS fg
	   ON (    fgf.FilterGroupId = fg.FilterGroupId 
		   and ISNULL(fgf.FilterGroupKeyFilter,'') = ISNULL(fg.FilterGroupKeyFilter,'') 
		   and ISNULL(fgf.FilterGroupFilterString,'') = ISNULL(fg.FilterGroupFilterString,''))
	 WHEN NOT MATCHED BY TARGET THEN
	 	  INSERT (FilterGroupId, FilterGroupKeyFilter, FilterGroupFilterString)
		  VALUES (fg.FilterGroupId, fg.FilterGroupKeyFilter, fg.FilterGroupFilterString);
	COMMIT TRANSACTION

	INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
		SELECT @DebugSessionId SessionId, 'MigrateTrackers' ApplicationName, SYSDATETIME() MessageDate, 'Extracting Measurement datapoints from XML documents' AS MessageText 
	IF (@DebugMode = 1)
	BEGIN
    	SELECT 'Extracting Measurement datapoints from XML documents at ' +  CONVERT(VARCHAR,SYSDATETIME()) AS TrackerDataLoad;
	END

	BEGIN TRANSACTION
	MERGE INTO TrackerData AS DataPoints
    USING (SELECT m.*, f.FilterGroupID, TT.TrackerTypeId, TT.TrackerTypeMinimumResolution, TM.TrackerMeasureId
		     FROM (SELECT DISTINCT td.Tracker_Xml_Document.value('(/TrackerDataPoint/TypeName/text())[1]', 'nvarchar(300)') 
				 		            + ISNULL(td.Tracker_Xml_Document.value('(/TrackerDataPoint/KeyFilter/text())[1]', 'nvarchar(300)'),'') TrackerId,
					                td.Tracker_Xml_Document.value('(/TrackerDataPoint/TypeName/text())[1]', 'nvarchar(300)') TypeName,
									td.Tracker_Xml_Document.value('(/TrackerDataPoint/TimeSlot)[1]', 'datetime') TimeSlotDt,
									convert(nvarchar(25),td.Tracker_Xml_Document.value('(/TrackerDataPoint/TimeSlot)[1]', 'datetime'),120) TimeSlotStr,
									ISNULL(td.Tracker_Xml_Document.value('(/TrackerDataPoint/KeyFilter/text())[1]', 'nvarchar(300)'),NULL) KeyFilter,
									tdtn.tnav.value('declare namespace d2p1="http://schemas.microsoft.com/2003/10/Serialization/Arrays";
						   							(d2p1:Key/text())[1]', 'nvarchar(300)') MeasurementName,
									tdtn.tnav.value('declare namespace d2p1="http://schemas.microsoft.com/2003/10/Serialization/Arrays"; 
									                (d2p1:Value/text())[1]', 'nvarchar(300)') MeasurementValue
					 FROM TrackerDocuments td
			  CROSS APPLY td.Tracker_Xml_Document.nodes('declare namespace d2p1="http://schemas.microsoft.com/2003/10/Serialization/Arrays";
					   									 declare namespace i="http://www.w3.org/2001/XMLSchema-instance";
														 /TrackerDataPoint/Measurement/d2p1:KeyValueOfstringlong') AS tdtn(tnav)
					WHERE TD.Document_Converted_By_Bulk_Load = 'N'
				  ) M,
				  FilterGroups f, TrackerTypes TT, TrackerMeasures tm
		    WHERE M.trackerId is not null
			  AND ISNULL(m.KeyFilter,'') = ISNULL(f.FilterGroupKeyFilter,'')
			  AND M.TypeName = TT.TrackerTypeName
			  and TT.TrackerTypeId = TM.TrackerTypeId
			  and m.MeasurementName = Tm.TrackerMeasureName) AS DMGM
	   ON DataPoints.TimeSlotDt = DMGM.TimeSlotDt
	  AND DataPoints.TrackerId = DMGM.TrackerId
	  AND DataPoints.TrackerTypeId = DMGM.TrackerTypeId
	  And DataPoints.DataResolution = DMGM.TrackerTypeMinimumResolution
	  AND ISNULL(DataPoints.KeyFilter,'') = ISNULL(DMGM.KeyFilter,'')
	  AND DataPoints.TrackerMeasureId = DMGM.TrackerMeasureId
	  AND DataPoints.FilterGroupId = DMGM.FilterGroupId
	 WHEN NOT MATCHED BY TARGET THEN
	 	  INSERT (TrackerId,TrackerTypeId, TimeSlotDt, TimeSlotStr,KeyFilter,TrackerMeasureId, MeasurementValue, FilterGroupID, DataResolution)
		  VALUES (DMGM.TrackerId,DMGM.TrackerTypeId, DMGM.TimeSlotDt,DMGM.TimeSlotStr, DMGM.KeyFilter,DMGM.TrackerMeasureId,DMGM.MeasurementValue,
				  DMGM.FilterGroupId, DMGM.TrackerTypeMinimumResolution)
	 WHEN MATCHED THEN
		  UPDATE SET DataPoints.MeasurementValue = DataPoints.MeasurementValue + DMGM.MeasurementValue;
	COMMIT TRANSACTION
	
    INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
		SELECT @DebugSessionId SessionId, 'MigrateTrackers' ApplicationName, SYSDATETIME() MessageDate, 'Done extracting data from documents' AS MessageText 
	IF (@DebugMode = 1)
	BEGIN
    	SELECT 'Done extracting data from documents ' +  CONVERT(VARCHAR,SYSDATETIME()) AS DoneExtractingDataPoints;
    	SELECT 'Determining minimum resolution values for Tracker Types ' +  CONVERT(VARCHAR,SYSDATETIME()) AS DeletingTrackerDataBuckets;
	END;
	INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
		SELECT @DebugSessionId SessionId, 'MigrateTrackers' ApplicationName, SYSDATETIME() MessageDate, 'Determining minimum resolution values for Tracker Types' AS MessageText 
	
	BEGIN TRANSACTION
	INSERT INTO @trackerTypesToDelete (TrackerTypeId)
	SELECT DISTINCT TT.TrackerTypeId
		     FROM (SELECT DISTINCT td.Tracker_Xml_Document.value('(/TrackerDataPoint/TypeName/text())[1]', 'nvarchar(300)') TypeName
					 FROM TrackerDocuments td
					WHERE TD.Document_Converted_By_Bulk_Load = 'N'
				  ) M, TrackerTypes TT
		    WHERE M.TypeName = TT.TrackerTypeName
	COMMIT TRANSACTION

    INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
		SELECT @DebugSessionId SessionId, 'MigrateTrackers' ApplicationName, SYSDATETIME() MessageDate, 'Updating minimum resolution for Tracker Types' AS MessageText 
	IF (@DebugMode = 1)
	BEGIN
    	SELECT 'Setting minimum resolution for Tracker Types at ' +  CONVERT(VARCHAR,SYSDATETIME()) AS CreatingTrackerDataBucketRecords;
        SELECT * FROM @trackerTypesToDelete
	END;
	INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
		SELECT @DebugSessionId SessionId, 'MigrateTrackers' ApplicationName, SYSDATETIME() MessageDate, 'Tracker Types before update:' AS MessageText 
	INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
		SELECT @DebugSessionId SessionId, 'MigrateTrackers' ApplicationName, SYSDATETIME() MessageDate, 'TrackerTypeId: '+ cast(TrackerTypeId as varchar(40)) + '    Resolution: ' + cast(TrackerResolution as varchar(20)) 
				AS MessageText  
		  FROM @trackerTypesToDelete;
	
	BEGIN TRANSACTION;

	WITH Datapoints AS
	(
		SELECT Row_Number() OVER (ORDER BY td.TrackerTypeId, TimeSlotDt) AS RowNumber, td.TrackerTypeId, TimeSlotDt
		  FROM TrackerData td, @trackerTypesToDelete tttd
		 WHERE timeSlotDt > '2014-09-01'
		   AND td.TrackerTypeId = tttd.TrackerTypeId
	  GROUP BY td.TrackerTypeId, TimeSlotDt
	)
	UPDATE tt set tt.TrackerTypeMinimumResolution = TR.Minimum_resolution_found
	FROM (SELECT TrackerTypeId, min(difference) minimum_resolution_found
			FROM (SELECT  cur.TrackerTypeId,  cur.TimeSlotDt currentDate, Prv.TimeSlotDt PriorDate
						  ,datediff(minute,prv.TimeSlotDt,cur.TimeSlotDt) AS Difference 
					FROM Datapoints Cur Left Outer Join Datapoints Prv
					  ON Cur.RowNumber =Prv.RowNumber + 1 
					 AND Cur.TrackerTypeId = Prv.TrackerTypeId
			  	 ) trackers
			WHERE difference > 0
		 GROUP BY TrackerTypeId
		 ) TR, TrackerTypes TT
	WHERE tt.TrackerTypeId = tr.TrackerTypeId

	UPDATE tttd  SET tttd.TrackerResolution = tt.TrackerTypeMinimumResolution
	FROM @trackerTypesToDelete tttd, TrackerTypes tt
    WHERE tttd.TrackerTypeId= tt.TrackerTypeId
	COMMIT TRANSACTION
	
    INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
		SELECT @DebugSessionId SessionId, 'MigrateTrackers' ApplicationName, SYSDATETIME() MessageDate, 'Tracker Types resolutions after update: ' AS MessageText 
	INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
		SELECT @DebugSessionId SessionId, 'MigrateTrackers' ApplicationName, SYSDATETIME() MessageDate, 'TrackerTypeId: '+ cast(TrackerTypeId as varchar(40)) + '    Resolution: ' + cast(TrackerResolution as varchar(20)) 
				AS MessageText  
		  FROM @trackerTypesToDelete;
	IF (@DebugMode = 1)
	BEGIN
        SELECT * FROM @trackerTypesToDelete
	END;

	IF (@DebugMode = 1)
	BEGIN
    	SELECT 'Updating Datapoints with extrapolated minimum resoltion:' +  CONVERT(VARCHAR,SYSDATETIME()) AS UpdatingDatapointResolutions;
	END
    INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
		SELECT @DebugSessionId SessionId, 'MigrateTrackers' ApplicationName, SYSDATETIME() MessageDate, 'Updating Datapoints with extrapolated minimum resoltion:' AS MessageText 

	BEGIN TRANSACTION

	UPDATE td  SET td.DataResolution = tt.TrackerTypeMinimumResolution
	FROM trackerData td, TrackerTypes tt
	WHERE td.TrackerTypeId= tt.TrackerTypeId
		AND td.DataResolution <> tt.TrackerTypeMinimumResolution
		AND td.dataResolution < 1440
		AND NOT EXISTS (SELECT NULL FROM TrackerData td2 
						WHERE td.TrackerTypeId = td2.trackerTypeId 
							AND TT.TrackerTypeId = Td2.TrackerTypeId 
							AND tt.TrackerTypeMinimumResolution = td2.dataresolution 
							AND td.filtergroupid = td2.filtergroupid
							AND td.TimeSlotDt = td2.TimeSlotDt
							AND td.TrackerMeasureId = td2.TrackerMeasureId
						)
		AND td.dataresolution = (SELECT MIN(dataResolution) FROM trackerdata td3 WHERE td.TrackerTypeId = td3.trackerTypeId 
							AND TT.TrackerTypeId = Td3.TrackerTypeId 
							AND tt.TrackerTypeMinimumResolution <= td3.dataresolution 
							AND td.filtergroupid = td3.filtergroupid
							AND td.TimeSlotDt = td3.TimeSlotDt
							AND td.TrackerMeasureId = td3.TrackerMeasureId)
	
	IF (@DebugMode = 1)
	BEGIN
    	SELECT 'Adding Tracker Types to BucketTrackerTypes for later bucket generation at:' +  CONVERT(VARCHAR,SYSDATETIME()) AS AddingBucketTrackerTypes;
	END
    INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
		SELECT @DebugSessionId SessionId, 'MigrateTrackers' ApplicationName, SYSDATETIME() MessageDate, 'Adding Tracker Types to BucketTrackerTypes for later bucket generation' AS MessageText 

	INSERT INTO BucketTrackerTypes (trackerTypeId, TrackerResolution)  
		 SELECT trackerTypeId, TrackerResolution 
		   FROM @trackerTypesToDelete tttd
		  WHERE NOT EXISTS (SELECT NULL FROM BucketTrackerTypes btt
							WHERE BTT.TrackerTypeId = tttd.TrackerTypeId);

	IF (@DebugMode = 1)
	BEGIN
    	SELECT 'Calling Generate Buckets at:' +  CONVERT(VARCHAR,SYSDATETIME()) AS CallingGenerateBuckets;
	END
    INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
		SELECT @DebugSessionId SessionId, 'MigrateTrackers' ApplicationName, SYSDATETIME() MessageDate, 'Calling Generate Buckets' AS MessageText 
	
	COMMIT TRANSACTION

	if (@GenerateBuckets = 1)
	BEGIN 
		EXEC GenerateBuckets;
	END

	BEGIN TRANSACTION
	INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
		SELECT @DebugSessionId SessionId, 'MigrateTrackers' ApplicationName, SYSDATETIME() MessageDate, 'Marking documents processed' AS MessageText 

	IF (@DebugMode = 1)
	BEGIN
		SELECT 'Marking documents processed at ' +  CONVERT(VARCHAR,SYSDATETIME()) AS MarkingDocumentsAsProcessed;
	END
	UPDATE TrackerDocuments SET Document_Converted_By_Bulk_Load = 'Y' WHERE Document_Converted_By_Bulk_Load = 'N';
	COMMIT TRANSACTION
	
	INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
		SELECT @DebugSessionId SessionId, 'MigrateTrackers' ApplicationName, SYSDATETIME() MessageDate, 'Document update complete' AS MessageText 

	IF (@DebugMode = 1)
	BEGIN
		SELECT 'Document update complete at ' +  CONVERT(VARCHAR,SYSDATETIME()) AS DocumentUpdateComplete;
	END

	IF (@DebugMode = 1)
	BEGIN
		SELECT 'Migration Complete at ' +  CONVERT(VARCHAR,SYSDATETIME()) AS MigrationComplete;
	END

	SELECT 
		@error = @@ERROR

	IF @error <> 0 
	BEGIN
		GOTO ERR_EXIT
	END
	
	OK_EXIT: 
		IF @return IS NULL SELECT @return = @RET_OK
		SET NOCOUNT OFF
		If (@IHaveTheLock = 1)
		   INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
			 SELECT @DebugSessionId SessionId, 'MigrateTrackers' ApplicationName, SYSDATETIME() MessageDate, 'Migration Complete' AS MessageText 
		RETURN @return
	
	ERR_EXIT:
		IF (@return IS NULL OR @return = 0) SELECT @return = @error
		If (@IHaveTheLock = 1)
		   INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
			 SELECT @DebugSessionId SessionId, 'MigrateTrackers' ApplicationName, SYSDATETIME() MessageDate, 'Migration Complete' AS MessageText 
		RETURN @return
END

GO
/****** Object:  StoredProcedure [dbo].[MigrateTrackersGenerateBuckets]    Script Date: 9/15/2015 10:33:20 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
--CREATE PROCEDURE [dbo].[MigrateTrackersGenerateBuckets] 
CREATE PROCEDURE [dbo].[MigrateTrackersGenerateBuckets] 
   AS 
BEGIN

	SET NOCOUNT ON
	
	DECLARE @resolutions TABLE (bigResolution INT, smallResolution INT)
	DECLARE @trackerTypesToDelete TABLE (TrackerTypeId	UNIQUEIDENTIFIER, TrackerResolution INT)

	DECLARE
		@error				INT,
		@return				INT,
		@RET_OK				INT,
		@DebugMode			INT,
		@biggerResolution	INT,
		@smallerResolution	INT,
		@UnMigratedCount    INT,
		@AllDocumentCount	INT,
		@lockStatus         VARCHAR(400),
		@IHaveTheLock       INT = 0,
		@UTCToLocalMidnightOffset int,
		@UTCTimeZoneMidnightOffset int

	INSERT INTO @resolutions (bigResolution,smallResolution)
		   (SELECT 43200,1440
				UNION
			SELECT 1440, 60
				UNION
			SELECT 60, 30
				UNION
			SELECT 30,15
				UNION 
			SELECT 15, 5
				UNION 
			SELECT 5,1	
			)

   	SET @RET_OK = 0
	SET @DebugMode = 1
	SET @UTCToLocalMidnightOffset = -8
	SET @UTCTimeZoneMidnightOffset = 8
	
    INSERT INTO MigrateLog (messageDate, MessageText)
		SELECT SYSDATETIME() MessageDate, 'Done extracting data from documents ' +  CONVERT(VARCHAR,SYSDATETIME()) AS MessageText 
	IF (@DebugMode = 1)
	BEGIN
    	SELECT 'Done extracting data from documents ' +  CONVERT(VARCHAR,SYSDATETIME()) AS EndOfMigration;
    	SELECT 'Determining minimum resolution values for Tracker Types ' +  CONVERT(VARCHAR,SYSDATETIME()) AS DeletingTrackerDataBuckets;
	END;
	INSERT INTO MigrateLog (messageDate, MessageText)
		SELECT SYSDATETIME() MessageDate, 'Determining minimum resolution values for Tracker Types ' +  CONVERT(VARCHAR,SYSDATETIME()) AS MessageText 
	
	BEGIN TRANSACTION
	INSERT INTO @trackerTypesToDelete (TrackerTypeId)
	SELECT DISTINCT TT.TrackerTypeId
		     FROM (SELECT DISTINCT td.Tracker_Xml_Document.value('(/TrackerDataPoint/TypeName/text())[1]', 'nvarchar(300)') TypeName
					 FROM TrackerDocuments td
					WHERE TD.Document_Converted_By_Bulk_Load = 'N'
				  ) M, TrackerTypes TT
		    WHERE M.TypeName = TT.TrackerTypeName
	COMMIT TRANSACTION

    INSERT INTO MigrateLog (messageDate, MessageText)
		SELECT SYSDATETIME() MessageDate, 'Updating minimum resolution for Tracker Types' AS MessageText 
	IF (@DebugMode = 1)
	BEGIN
    	SELECT 'Setting minimum resolution for Tracker Types at ' +  CONVERT(VARCHAR,SYSDATETIME()) AS CreatingTrackerDataBucketRecords;
        SELECT * FROM @trackerTypesToDelete
	END;
	INSERT INTO MigrateLog (messageDate, MessageText)
		SELECT SYSDATETIME() MessageDate, 'Tracker Types before update:' AS MessageText 
	INSERT INTO MigrateLog (messageDate, MessageText)
		SELECT SYSDATETIME() MessageDate, 'TrackerTypeId: '+ cast(TrackerTypeId as varchar(40)) + '    Resolution: ' + cast(TrackerResolution as varchar(20)) 
				AS MessageText  
		  FROM @trackerTypesToDelete;
	
	BEGIN TRANSACTION;

	WITH Datapoints AS
	(
		SELECT Row_Number() OVER (ORDER BY td.TrackerTypeId, TimeSlotDt) AS RowNumber, td.TrackerTypeId, TimeSlotDt
		  FROM TrackerData td, @trackerTypesToDelete tttd
		 WHERE timeSlotDt > '2014-09-01'
		   AND td.TrackerTypeId = tttd.TrackerTypeId
	  GROUP BY td.TrackerTypeId, TimeSlotDt
	)
	UPDATE tt set tt.TrackerTypeMinimumResolution = TR.Minimum_resolution_found
	FROM (SELECT TrackerTypeId, min(difference) minimum_resolution_found
			FROM (SELECT  cur.TrackerTypeId,  cur.TimeSlotDt currentDate, Prv.TimeSlotDt PriorDate
						  ,datediff(minute,prv.TimeSlotDt,cur.TimeSlotDt) AS Difference 
					FROM Datapoints Cur Left Outer Join Datapoints Prv
					  ON Cur.RowNumber =Prv.RowNumber + 1 
					 AND Cur.TrackerTypeId = Prv.TrackerTypeId
			  	 ) trackers
			WHERE difference > 0
		 GROUP BY TrackerTypeId
		 ) TR, TrackerTypes TT
	WHERE tt.TrackerTypeId = tr.TrackerTypeId

	UPDATE tttd  SET tttd.TrackerResolution = tt.TrackerTypeMinimumResolution
	FROM @trackerTypesToDelete tttd, TrackerTypes tt
    WHERE tttd.TrackerTypeId= tt.TrackerTypeId
	COMMIT TRANSACTION
	
    INSERT INTO MigrateLog (messageDate, MessageText)
		SELECT SYSDATETIME() MessageDate, 'Tracker Types resolutions after update: ' AS MessageText 
	INSERT INTO MigrateLog (messageDate, MessageText)
		SELECT SYSDATETIME() MessageDate, 'TrackerTypeId: '+ cast(TrackerTypeId as varchar(40)) + '    Resolution: ' + cast(TrackerResolution as varchar(20)) 
				AS MessageText  
		  FROM @trackerTypesToDelete;
	IF (@DebugMode = 1)
	BEGIN
        SELECT * FROM @trackerTypesToDelete
	END;

	IF (@DebugMode = 1)
	BEGIN
    	SELECT 'Removing higher resolution total records at:' +  CONVERT(VARCHAR,SYSDATETIME()) AS ClearingHigherResolutions;
	END
    INSERT INTO MigrateLog (messageDate, MessageText)
		SELECT SYSDATETIME() MessageDate, 'Removing higher resolution total records at:' +  CONVERT(VARCHAR,SYSDATETIME()) AS MessageText 

	BEGIN TRANSACTION
	UPDATE td  SET td.DataResolution = tt.TrackerTypeMinimumResolution
	FROM trackerData td, TrackerTypes tt
    WHERE td.TrackerTypeId= tt.TrackerTypeId
	  AND td.DataResolution <> tt.TrackerTypeMinimumResolution
	  AND td.dataResolution < 1440
	  AND NOT EXISTS (SELECT NULL FROM TrackerData td2 
					   WHERE td.TrackerTypeId = td2.trackerTypeId 
						 AND TT.TrackerTypeId = Td2.TrackerTypeId 
						 AND tt.TrackerTypeMinimumResolution = td2.dataresolution 
						 AND td.filtergroupid = td2.filtergroupid
						 AND td.TimeSlotDt = td2.TimeSlotDt
						 AND td.TrackerMeasureId = td2.TrackerMeasureId
					 )
	  AND td.dataresolution = (SELECT MIN(dataResolution) FROM trackerdata td3 WHERE td.TrackerTypeId = td3.trackerTypeId 
						 AND TT.TrackerTypeId = Td3.TrackerTypeId 
						 AND tt.TrackerTypeMinimumResolution <= td3.dataresolution 
						 AND td.filtergroupid = td3.filtergroupid
						 AND td.TimeSlotDt = td3.TimeSlotDt
						 AND td.TrackerMeasureId = td3.TrackerMeasureId)
	
	DELETE TD FROM TrackerData td, DeleteTrackerTypes tttd 
	 WHERE td.TrackerTypeId = tttd.TrackerTypeId
       AND td.DataResolution <> tttd.trackerResolution
    COMMIT TRANSACTION

	INSERT INTO MigrateLog (messageDate, MessageText)
		SELECT SYSDATETIME() MessageDate, 'Generating 15 minute total datapoints from 5 minute resolutions' +  CONVERT(VARCHAR,SYSDATETIME()) AS MessageText 
	IF (@DebugMode = 1)
	BEGIN
		SELECT 'Generating 15 minute total datapoints from 5 minute resolutions' +  CONVERT(VARCHAR,SYSDATETIME()) AS Adding15MinuteTotals;
	END
	
	BEGIN TRANSACTION
	INSERT INTO TrackerData (TimeSlotDt,MeasurementValue,TrackerId, TrackerTypeId, TimeSlotStr, KeyFilter, TrackerMeasureId, FilterGroupID, DataResolution )
	SELECT DATEADD(mi,(15- ((DATEPART(minute,dateadd(mi,-7.5,mgm.TimeSlotDt)) - ( 15 *(DATEPART(minute,DATEADD(mi,-1,dateadd(mi,-7.5,mgm.TimeSlotDt)))/ 15))))),dateadd(mi,-7.5,mgm.TimeSlotDt)) Adjusted_15_Minute_date
			, SUM(MeasurementValue) AS MeasurementValue
			, TrackerId, mgm.TrackerTypeId
			, convert(varchar(20),DATEADD(mi,(15- ((DATEPART(minute,dateadd(mi,-7.5,mgm.TimeSlotDt)) - ( 15 *(DATEPART(minute,DATEADD(mi,-1,dateadd(mi,-7.5,mgm.TimeSlotDt)))/ 15))))),dateadd(mi,-7.5,mgm.TimeSlotDt)),120) Adjusted_date_str
			, KeyFilter, mgm.TrackerMeasureId, mgm.FilterGroupID, 15 as DataResolution
		FROM dbo.TrackerData mgm WITH (NOLOCK), @trackerTypesToDelete tttd
		WHERE mgm.DataResolution = 5
		  AND mgm.TrackerTypeId = tttd.TrackerTypeId
		  AND tttd.TrackerResolution <> 15
		GROUP BY DATEADD(mi,(15- ((DATEPART(minute,dateadd(mi,-7.5,mgm.TimeSlotDt)) - ( 15 *(DATEPART(minute,DATEADD(mi,-1,dateadd(mi,-7.5,mgm.TimeSlotDt)))/ 15))))),dateadd(mi,-7.5,mgm.TimeSlotDt)),
				 TrackerId, mgm.TrackerTypeId, KeyFilter, mgm.TrackerMeasureId, mgm.FilterGroupID,
				 convert(varchar(20),DATEADD(mi,(15- ((DATEPART(minute,dateadd(mi,-7.5,mgm.TimeSlotDt)) - ( 15 *(DATEPART(minute,DATEADD(mi,-1,dateadd(mi,-7.5,mgm.TimeSlotDt)))/ 15))))),dateadd(mi,-7.5,mgm.TimeSlotDt)),120) 
		ORDER BY 1
	COMMIT TRANSACTION

	INSERT INTO MigrateLog (messageDate, MessageText)
		SELECT SYSDATETIME() MessageDate, 'Generating 30 minute total datapoints from 15 minute resolutions' +  CONVERT(VARCHAR,SYSDATETIME()) AS MessageText 
	IF (@DebugMode = 1)
	BEGIN
    	SELECT 'Generating 30 minute total datapoints from 15 minute resolutions' +  CONVERT(VARCHAR,SYSDATETIME()) AS Generate30MinuteTotals;
	END

	BEGIN TRANSACTION
	INSERT INTO TrackerData (mgm.TimeSlotDt,MeasurementValue,TrackerId, TrackerTypeId, TimeSlotStr, KeyFilter, TrackerMeasureId, FilterGroupID, DataResolution )
	SELECT CASE WHEN DATEPART(minute,mgm.TimeSlotDt) = 0 THEN mgm.TimeSlotDt
			ELSE DATEADD(mi,(30- ((DATEPART(minute,mgm.TimeSlotDt) - ( 30 *(DATEPART(minute,DATEADD(mi,-1,mgm.TimeSlotDt))/ 30))))),mgm.TimeSlotDt) 
			END Adjusted_30_Minute_date
			, SUM(MeasurementValue) AS MeasurementValue
			, TrackerId, mgm.TrackerTypeId
			, CASE WHEN DATEPART(minute,mgm.TimeSlotDt) = 0 then convert(varchar(20),(mgm.TimeSlotDt),120)
			ELSE convert(varchar(20),(DATEADD(mi,(30- ((DATEPART(minute,mgm.TimeSlotDt) - ( 30 *(DATEPART(minute,DATEADD(mi,-1,mgm.TimeSlotDt))/ 30))))),mgm.TimeSlotDt) ),120)
			END Adjusted_date_str
			, KeyFilter, mgm.TrackerMeasureId, mgm.FilterGroupID, 30 as DataResolution
			FROM dbo.TrackerData mgm WITH (NOLOCK), @trackerTypesToDelete tttd
		WHERE mgm.DataResolution = 15
		  AND mgm.TrackerTypeId = tttd.TrackerTypeId
		  AND tttd.TrackerResolution <> 30
		GROUP BY CASE WHEN DATEPART(minute,mgm.TimeSlotDt) = 0 then mgm.TimeSlotDt
						ELSE DATEADD(mi,(30- ((DATEPART(minute,mgm.TimeSlotDt) - ( 30 *(DATEPART(minute,DATEADD(mi,-1,mgm.TimeSlotDt))/ 30))))),mgm.TimeSlotDt) 
					END
				 , TrackerId, mgm.TrackerTypeId, KeyFilter, mgm.TrackerMeasureId, mgm.FilterGroupID,
				 CASE WHEN DATEPART(minute,mgm.TimeSlotDt) = 0 then convert(varchar(20),(mgm.TimeSlotDt),120)
				      ELSE convert(varchar(20),(DATEADD(mi,(30- ((DATEPART(minute,mgm.TimeSlotDt) - ( 30 *(DATEPART(minute,DATEADD(mi,-1,mgm.TimeSlotDt))/ 30))))),mgm.TimeSlotDt) ),120)
					END 
		ORDER BY 1
	COMMIT TRANSACTION

	INSERT INTO MigrateLog (messageDate, MessageText)
		SELECT SYSDATETIME() MessageDate, 'Generating hour total datapoints from 15 minute resolutions' +  CONVERT(VARCHAR,SYSDATETIME()) AS MessageText 
	IF (@DebugMode = 1)
	BEGIN
    	SELECT 'Generating hour total datapoints from 15 minute resolutions' +  CONVERT(VARCHAR,SYSDATETIME()) AS GeneratingHourTotals;
	END

	BEGIN TRANSACTION
	INSERT INTO TrackerData (mgm.TimeSlotDt,MeasurementValue,TrackerId, TrackerTypeId, TimeSlotStr, KeyFilter, TrackerMeasureId, FilterGroupID, DataResolution )
		SELECT cast(convert(varchar(13),mgm.TimeSlotDt,120) + ':00' as datetime) Adjusted_date
				, SUM(MeasurementValue) AS MeasurementValue
				, TrackerId, mgm.TrackerTypeId
				, convert(varchar(20),(cast(convert(varchar(13),mgm.TimeSlotDt,120) + ':00' as datetime)),120) Adjusted_date_str
				, KeyFilter, mgm.TrackerMeasureId, mgm.FilterGroupID, 60 as DataResolution
			FROM dbo.TrackerData mgm WITH (NOLOCK), @trackerTypesToDelete tttd
		WHERE mgm.DataResolution = 15
		  AND mgm.TrackerTypeId = tttd.TrackerTypeId
		  AND tttd.TrackerResolution <> 60
		GROUP BY cast(convert(varchar(13),mgm.TimeSlotDt,120) + ':00' as datetime)
 				, TrackerId, mgm.TrackerTypeId, KeyFilter, mgm.TrackerMeasureId, mgm.FilterGroupID,
				convert(varchar(20),(cast(convert(varchar(13),mgm.TimeSlotDt,120) + ':00' as datetime)),120)
		ORDER BY 1
	COMMIT TRANSACTION

	INSERT INTO MigrateLog (messageDate, MessageText)
		SELECT SYSDATETIME() MessageDate, 'Generating day total datapoints from hour resolutions' +  CONVERT(VARCHAR,SYSDATETIME()) AS MessageText 
	IF (@DebugMode = 1)
	BEGIN
    	SELECT 'Generating day total datapoints from hour resolutions' +  CONVERT(VARCHAR,SYSDATETIME()) AS GeneratingDayTotals;
	END

	BEGIN TRANSACTION
	INSERT INTO TrackerData (mgm.TimeSlotDt,MeasurementValue,TrackerId, TrackerTypeId, TimeSlotStr, KeyFilter, TrackerMeasureId, FilterGroupID, DataResolution )
		SELECT dateadd(hour,@UTCTimeZoneMidnightOffset,cast(convert(varchar(10),dateadd(hour,@UTCToLocalMidnightOffset, mgm.TimeSlotDt),120) as datetime)) Adjusted_date
				, SUM(MeasurementValue) AS MeasurementValue
				, TrackerId, mgm.TrackerTypeId
				, convert(varchar(20),(dateadd(hour,@UTCTimeZoneMidnightOffset,cast(convert(varchar(10),dateadd(hour,@UTCToLocalMidnightOffset, mgm.TimeSlotDt),120) as datetime))),120) Adjusted_date_str
				, KeyFilter, mgm.TrackerMeasureId, mgm.FilterGroupID, 1440 as DataResolution
			FROM dbo.TrackerData mgm WITH (NOLOCK), @trackerTypesToDelete tttd
		WHERE mgm.DataResolution = 60
		  AND mgm.TrackerTypeId = tttd.TrackerTypeId
		  AND tttd.TrackerResolution <> 1440
	GROUP BY dateadd(hour,@UTCTimeZoneMidnightOffset,cast(convert(varchar(10),dateadd(hour,@UTCToLocalMidnightOffset, mgm.TimeSlotDt),120) as datetime))
	  			, TrackerId, mgm.TrackerTypeId, KeyFilter, mgm.TrackerMeasureId, mgm.FilterGroupID,
			 convert(varchar(20),(dateadd(hour,@UTCTimeZoneMidnightOffset,cast(convert(varchar(10),dateadd(hour,@UTCToLocalMidnightOffset, mgm.TimeSlotDt),120) as datetime))),120)
	ORDER BY 1
	COMMIT TRANSACTION

	INSERT INTO MigrateLog (messageDate, MessageText)
		SELECT SYSDATETIME() MessageDate, 'Generating month total datapoints from day resolutions' +  CONVERT(VARCHAR,SYSDATETIME()) as MessageText 
	IF (@DebugMode = 1)
	BEGIN
    	SELECT 'Generating month total datapoints from day resolutions' +  CONVERT(VARCHAR,SYSDATETIME()) AS GeneratingMonthTotals;
	END
	
	BEGIN TRANSACTION
	INSERT INTO TrackerData (mgm.TimeSlotDt,MeasurementValue,TrackerId, TrackerTypeId, TimeSlotStr, KeyFilter, TrackerMeasureId, FilterGroupID, DataResolution )
			SELECT dateadd(hour,@UTCTimeZoneMidnightOffset,cast(convert(varchar(7),dateadd(hour,@UTCToLocalMidnightOffset, mgm.TimeSlotDt),120) + '-01' as datetime)) Adjusted_date
				, SUM(MeasurementValue) AS MeasurementValue
				, TrackerId, mgm.TrackerTypeId
				, convert(varchar(20),(dateadd(hour,@UTCTimeZoneMidnightOffset,cast(convert(varchar(7),dateadd(hour,@UTCToLocalMidnightOffset, mgm.TimeSlotDt),120)+ '-01' as datetime))),120) Adjusted_date_str
				, KeyFilter, mgm.TrackerMeasureId, mgm.FilterGroupID, 43200 as DataResolution
			FROM dbo.TrackerData mgm WITH (NOLOCK), @trackerTypesToDelete tttd
		WHERE mgm.DataResolution = 1440
		  AND mgm.TrackerTypeId = tttd.TrackerTypeId
		  AND tttd.TrackerResolution <> 43200
		GROUP BY dateadd(hour,@UTCTimeZoneMidnightOffset,cast(convert(varchar(7),dateadd(hour,@UTCToLocalMidnightOffset, mgm.TimeSlotDt),120)+ '-01' as datetime)) 
				, TrackerId, mgm.TrackerTypeId, KeyFilter, mgm.TrackerMeasureId, mgm.FilterGroupID
				, convert(varchar(20),(dateadd(hour,@UTCTimeZoneMidnightOffset,cast(convert(varchar(7),dateadd(hour,@UTCToLocalMidnightOffset, mgm.TimeSlotDt),120)+ '-01' as datetime))),120)
		ORDER BY 1
	COMMIT TRANSACTION

	INSERT INTO MigrateLog (messageDate, MessageText)
		SELECT SYSDATETIME() MessageDate, 'Generating smaller datapoints from bigger resolution datapoints' +  CONVERT(VARCHAR,SYSDATETIME()) as MessageText 
	IF (@DebugMode = 1)
	BEGIN
    	SELECT 'Generating smaller datapoints from bigger resolution datapoints' +  CONVERT(VARCHAR,SYSDATETIME()) AS StartingSmallerResolutionTotals;
	END

	WHILE (SELECT COUNT(*) FROM @RESOLUTIONS) > 0
	BEGIN
		SELECT TOP 1 @biggerResolution = bigResolution, @smallerResolution = smallResolution 
			FROM @resolutions 
		ORDER BY bigResolution DESC

		INSERT INTO MigrateLog (messageDate, MessageText
)
			SELECT SYSDATETIME() MessageDate, 'Using ' + CAST(@biggerResolution AS varchar) + ' datapoints to generate ' + 
					CAST(@smallerResolution as varchar) + ' datapoints at:' +  CONVERT(VARCHAR,SYSDATETIME()) as MessageText 
		IF (@DebugMode = 1)
		BEGIN
			SELECT 'Using ' + CAST(@biggerResolution AS varchar) + ' datapoints to generate ' + CAST(@smallerResolution as varchar) +
				   ' datapoints at:' +  CONVERT(VARCHAR,SYSDATETIME()) AS GeneratingSmallerPlaceholderTotalRecords;
		END

		BEGIN TRANSACTION
		INSERT INTO TrackerData (TimeSlotDt,MeasurementValue,TrackerId, TrackerTypeId, TimeSlotStr, KeyFilter, TrackerMeasureId, FilterGroupID, DataResolution )
				SELECT TimeSlotDt
					, MeasurementValue
					, TrackerId, td_outer.TrackerTypeId
					, TimeSlotStr
					, KeyFilter, td_outer.TrackerMeasureId, td_outer.FilterGroupID, @smallerResolution as DataResolution
				FROM dbo.TrackerData td_outer WITH (NOLOCK), @trackerTypesToDelete tttd
				WHERE td_outer.DataResolution = @biggerResolution
				  AND td_outer.TrackerTypeId = tttd.TrackerTypeId
				  AND NOT EXISTS (SELECT NULL 
									FROM TrackerData td_inner
								   WHERE td_outer.FilterGroupID = td_inner.FilterGroupID
 									 AND td_outer.KeyFilter = td_inner.KeyFilter
									 AND td_inner.TimeSlotDt BETWEEN td_outer.TimeSlotDt AND CASE WHEN @biggerResolution = 43200 THEN DATEADD(MONTH,1,td_outer.TimeSlotDt)
																								  WHEN @biggerResolution = 1440 THEN DATEADD(DAY,1,td_outer.TimeSlotDt)
																								  WHEN @biggerResolution = 60 THEN DATEADD(HOUR,1,td_outer.TimeSlotDt)
																								  ELSE DATEADD(MINUTE,@biggerResolution,td_outer.TimeSlotDt) 
																							  END
									 AND td_outer.TrackerMeasureId = td_inner.TrackerMeasureId
									 AND td_outer.TrackerTypeId = td_inner.TrackerTypeId
									 AND td_outer.TrackerId = td_inner.TrackerId
									 AND td_inner.DataResolution = @smallerResolution
								)
		COMMIT TRANSACTION

		DELETE FROM @resolutions WHERE bigResolution = @biggerResolution AND smallResolution = @smallerResolution
	END

    INSERT INTO MigrateLog (messageDate, MessageText)
		SELECT SYSDATETIME() MessageDate, 'Rebuilding indexes at ' +  CONVERT(VARCHAR,SYSDATETIME()) AS MessageText 
	IF (@DebugMode = 1)
	BEGIN
		SELECT 'Rebuilding indexes at ' +  CONVERT(VARCHAR,SYSDATETIME()) AS RebuildingIndexes;
	END
	ALTER INDEX [IX_TrackerData] ON [dbo].[TrackerData] REBUILD PARTITION = ALL 
	ALTER INDEX [IX_TrackerData_TrackerType] ON [dbo].[TrackerData] REBUILD PARTITION = ALL 
	ALTER INDEX [PK_TrackerData] ON [dbo].[TrackerData] REBUILD PARTITION = ALL 
	ALTER INDEX [IX_FilterGroupFilters_FilterString] ON [dbo].[FilterGroupFilters] REBUILD PARTITION = ALL 
	ALTER INDEX [IX_FilterGroupFilters_KeyFilter] ON [dbo].[FilterGroupFilters] REBUILD PARTITION = ALL 
	
    INSERT INTO MigrateLog (messageDate, MessageText)
		SELECT SYSDATETIME() MessageDate, 'Index Rebuild finished at ' +  CONVERT(VARCHAR,SYSDATETIME()) AS MessageText 

	IF (@DebugMode = 1)
	BEGIN
		SELECT 'Index Rebuild finished at ' +  CONVERT(VARCHAR,SYSDATETIME()) AS IndexRebuildComplete;
	END

	INSERT INTO MigrateLog (messageDate, MessageText)
		SELECT SYSDATETIME() MessageDate, 'Marking documents processed at ' +  CONVERT(VARCHAR,SYSDATETIME()) AS MessageText 

	IF (@DebugMode = 1)
	BEGIN
		SELECT 'Marking documents processed at ' +  CONVERT(VARCHAR,SYSDATETIME()) AS MarkingDocumentsAsProcessed;
	END
	BEGIN TRANSACTION
	UPDATE TrackerDocuments SET Document_Converted_By_Bulk_Load = 'Y' WHERE Document_Converted_By_Bulk_Load = 'N';
	COMMIT TRANSACTION
	
	INSERT INTO MigrateLog (messageDate, MessageText)
		SELECT SYSDATETIME() MessageDate, 'Document update complete at ' +  CONVERT(VARCHAR,SYSDATETIME()) AS MessageText 

	IF (@DebugMode = 1)
	BEGIN
		SELECT 'Document update complete at ' +  CONVERT(VARCHAR,SYSDATETIME()) AS MigrationComplete;
	END

    INSERT INTO MigrateLog (messageDate, MessageText)
		SELECT SYSDATETIME() MessageDate, 'Migration Complete at ' +  CONVERT(VARCHAR,SYSDATETIME()) AS MessageText 

	IF (@DebugMode = 1)
	BEGIN
		SELECT 'Migration Complete at ' +  CONVERT(VARCHAR,SYSDATETIME()) AS MigrationComplete;
	END

	SELECT 
		@error = @@ERROR

	IF @error <> 0 
	BEGIN
		GOTO ERR_EXIT
	END
	
	OK_EXIT: 
		IF @return IS NULL SELECT @return = @RET_OK
		SET NOCOUNT OFF
		If (@IHaveTheLock = 1)
		   INSERT INTO MigrateLog (messageDate, MessageText)
			 SELECT SYSDATETIME() MessageDate, 'Migration Complete at ' +  CONVERT(VARCHAR,SYSDATETIME()) AS MessageText 
		RETURN @return
	
	ERR_EXIT:
		IF (@return IS NULL OR @return = 0) SELECT @return = @error
		If (@IHaveTheLock = 1)
		   INSERT INTO MigrateLog (messageDate, MessageText)
			 SELECT SYSDATETIME() MessageDate, 'Migration Complete at ' +  CONVERT(VARCHAR,SYSDATETIME()) AS MessageText 
		RETURN @return
END


GO
/****** Object:  StoredProcedure [dbo].[UpdateTracker]    Script Date: 9/15/2015 10:33:21 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




/****** Object:  StoredProcedure [dbo].[UpdateTracker]    Script Date: 4/14/2015 2:06:28 PM ******/
CREATE PROCEDURE [dbo].[UpdateTracker] 
    @TrackerID NVARCHAR (450), 
    @Name NVARCHAR(250), 
    @TypeName NVARCHAR(150), 
    @MinResolution INT, 
    @KeyFilter NVARCHAR(450), 
    @TimeSlot DATETIME, 
    @FilterList AS dbo.FilterList READONLY, 
    @Measurement AS dbo.Measurement READONLY
AS 
BEGIN
	
	SET NOCOUNT ON

	DECLARE 
		@error				int,
		@return				int,
		@RET_OK				int,
		@TrackerTypeId		uniqueidentifier,
		@KeyFilterPassed	NVARCHAR(450),
		@DebugMode			INT,
		@DebugId			uniqueidentifier,
		@DebugIdString		nvarchar(50),
		@Measurements       dbo.Measurement,
		@TypeResolution     int
	
	SELECT 
		@RET_OK = 0

	SET @DebugMode = 1
	SET @DebugId = newid()
	set @DebugIdString = convert(nvarchar(50), @debugid)

	BEGIN TRANSACTION

	IF (@DebugMode = 1)
	BEGIN
		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
				SELECT @DebugId SessionId, 'UpdateTracker' ApplicationName, SYSDATETIME() MessageDate, ' Update Tracker called with TrackerId : ' +  cast(@TrackerId as varchar(360))  AS MessageText;

		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
				SELECT @DebugId SessionId, 'UpdateTracker' ApplicationName, SYSDATETIME() MessageDate, '                             TimeSlot : ' +  convert(VARCHAR,@TimeSlot)  AS MessageText;

		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
				SELECT @DebugId SessionId, 'UpdateTracker' ApplicationName, SYSDATETIME() MessageDate, '                            Name : ' +  @Name  AS MessageText;

		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
				SELECT @DebugId SessionId, 'UpdateTracker' ApplicationName, SYSDATETIME() MessageDate, '                            TypeName : ' +  @TypeName  AS MessageText;
	
		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
				SELECT @DebugId SessionId, 'UpdateTracker' ApplicationName, SYSDATETIME() MessageDate, '                            MinResolution : ' +  Cast(@MinResolution as varchar(10)) AS MessageText;

		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
				SELECT @DebugId SessionId, 'UpdateTracker' ApplicationName, SYSDATETIME() MessageDate, '                            KeyFilter : ' +  cast(@KeyFilter as varchar(360))  AS MessageText;

		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
			SELECT @DebugId SessionId, 'UpdateTracker' ApplicationName, SYSDATETIME() MessageDate, '         Filter list count = ' + convert( nvarchar(10),count(*))   AS MessageText  
			  FROM @FilterList;

		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
			SELECT @DebugId SessionId, 'UpdateTracker' ApplicationName, SYSDATETIME() MessageDate, '                                    Filter: '+ cast(Filter as varchar(360)) AS MessageText  
			  FROM @FilterList;

		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
			SELECT @DebugId SessionId, 'UpdateTracker' ApplicationName, SYSDATETIME() MessageDate, '         Measurement count = ' + convert( nvarchar(10),count(*))   AS MessageText  
			  FROM @Measurement;

		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText)
			SELECT @DebugId SessionId, 'UpdateTracker' ApplicationName, SYSDATETIME() MessageDate, '         Measurements     Name: ' + Name + '  Value: ' + convert(varchar(30),value) + ' Bucket Res: ' + convert(varchar(30),BucketResolution)  AS MessageText  
			  FROM @Measurement;
	END;
	COMMIT TRANSACTION

	BEGIN TRANSACTION

	IF @KeyFilter = '' or @KeyFilter is null
	   SET @KeyFilterPassed = NULL
	ELSE
	   SET @KeyFilterPassed = @KeyFilter
	IF (select count(*) from @measurement) = 0
	BEGIN
		RAISERROR (N'UpdateTracker called with no measures to record.',1,1);
		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText) SELECT @DebugId SessionId, 'UpdateTracker' ApplicationName, SYSDATETIME() MessageDate, ' NO MEASUREMENTS RECEIVED!!!!!!!!!!';
		SELECT @DebugId SessionId, 'UpdateTracker' ApplicationName, SYSDATETIME() MessageDate, ' NO MEASUREMENTS RECEIVED!!!!!!!!!!';
		COMMIT TRANSACTION
		GOTO ERR_EXIT
	END

	IF ((select count(*) from @FilterList) = 0 AND @KeyFilterPassed is not null) OR
		((select count(*) from @FilterList) > 0 AND @KeyFilterPassed is null)
	BEGIN
		RAISERROR (N'UpdateTracker called with Filter and KeyFilter mismatch.',1,1);
		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText) SELECT @DebugId SessionId, 'UpdateTracker' ApplicationName, SYSDATETIME() MessageDate, ' UpdateTracker called with Filter and KeyFilter mismatch.';
		SELECT @DebugId SessionId, 'UpdateTracker' ApplicationName, SYSDATETIME() MessageDate, ' UpdateTracker called with Filter and KeyFilter mismatch.';
		COMMIT TRANSACTION
		GOTO ERR_EXIT
	END

	IF (@DebugMode = 1)
	BEGIN
		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText) SELECT @DebugId SessionId, 'UpdateTracker' ApplicationName, SYSDATETIME() MessageDate, ' Merging into TrackerTypes';
		SELECT @DebugId SessionId, 'UpdateTracker' ApplicationName, SYSDATETIME() MessageDate, ' Merging into TrackerTypes';
	END;
	
	insert into @measurements Select * from @measurement

	IF (@MinResolution <= 0)
	   set @TypeResolution = 5;
	ELSE
		set @TypeResolution = @MinResolution;

	MERGE INTO TrackerTypes AS TT
	USING (select @TypeName TypeName) as tfd
		ON tt.TrackerTypeName = tfd.TypeName
		WHEN NOT MATCHED BY TARGET THEN
	   		INSERT (TrackerTypeName, TrackerTypeDescription, TrackerTypeMinimumResolution)
			VALUES (tfd.TypeName, tfd.TypeName, @TypeResolution);

	select @TrackerTypeId = TrackerTypeId from TrackerTypes where TrackerTypeName = @TypeName

	IF (@DebugMode = 1)
	BEGIN
		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText) SELECT @DebugId SessionId, 'UpdateTracker' ApplicationName, SYSDATETIME() MessageDate, ' TrackerTypeID ' + convert(Nvarchar(50),@TrackerTypeId);
		SELECT @DebugId SessionId, 'UpdateTracker' ApplicationName, SYSDATETIME() MessageDate, ' TrackerTypeID ' + convert(Nvarchar(50),@TrackerTypeId);
	END;
	IF (@MinResolution <= 0)
	BEGIN
		SELECT @MinResolution = TrackerTypeMinimumResolution
		  FROM TrackerTypes
		 WHERE TrackerTypeName = @TypeName

		IF (@MinResolution <= 0)
		BEGIN
			WITH Datapoints AS
			(
				SELECT Row_Number() OVER (ORDER BY td.TrackerTypeId, TimeSlotDt) AS RowNumber, td.TrackerTypeId, TimeSlotDt
				  FROM TrackerData td
				 WHERE timeSlotDt > '2014-09-01'
				   AND td.TrackerTypeId = @TrackerTypeId
			  GROUP BY td.TrackerTypeId, TimeSlotDt
			)
			UPDATE tt set tt.TrackerTypeMinimumResolution = TR.Minimum_resolution_found
				FROM (SELECT TrackerTypeId, min(difference) minimum_resolution_found
						FROM (SELECT  cur.TrackerTypeId,  cur.TimeSlotDt currentDate, Prv.TimeSlotDt PriorDate
									  ,datediff(minute,prv.TimeSlotDt,cur.TimeSlotDt) AS Difference 
								FROM Datapoints Cur Left Outer Join Datapoints Prv
								  ON Cur.RowNumber =Prv.RowNumber + 1 
								 AND Cur.TrackerTypeId = Prv.TrackerTypeId
								 AND @TrackerTypeID = Cur.TrackerTypeId
								 AND @TrackerTypeID = Prv.TrackerTypeId
			  				 ) trackers
						WHERE difference > 0
					 GROUP BY TrackerTypeId
					 ) TR, TrackerTypes TT
				WHERE tt.TrackerTypeId = tr.TrackerTypeId
				  AND tt.TrackerTypeId = @TrackerTypeId 
				  AND TR.TrackerTypeId = @TrackerTypeId

			SELECT @MinResolution = TrackerTypeMinimumResolution
			  FROM TrackerTypes
			 WHERE TrackerTypeName = @TypeName
		END
        
		IF (@MinResolution <= 0)
		BEGIN
			SET @MinResolution = 5;
		END
		update @Measurements set BucketResolution = @MinResolution
	END

	IF (@DebugMode = 1)
	BEGIN
		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText) SELECT @DebugId SessionId, 'UpdateTracker' ApplicationName, SYSDATETIME() MessageDate, ' MinResolution = ' + convert(nvarchar(10),@MinResolution);
		SELECT @DebugId SessionId, 'UpdateTracker' ApplicationName, SYSDATETIME() MessageDate, ' MinResolution = ' + convert(nvarchar(10),@MinResolution);
	END;		

	IF (@DebugMode = 1)
	BEGIN
		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText) SELECT @DebugId SessionId, 'UpdateTracker' ApplicationName, SYSDATETIME() MessageDate, ' Merging into TrackerMeasures';
		SELECT @DebugId SessionId, 'UpdateTracker' ApplicationName, SYSDATETIME() MessageDate, ' Merging into TrackerMeasures';
	END;
	MERGE INTO TrackerMeasures AS TM
	USING (select name as TrackerMeasureName, @TrackerTypeId TrackerTypeId
	        from @Measurements
			) TTMD
		ON ttmd.TrackerTypeId = TM.TrackerTypeId
	    AND ttmd.TrackerMeasureName = tm.TrackerMeasureName
	    WHEN NOT MATCHED BY TARGET THEN
	   	    INSERT (TrackerMeasureName, TrackerTypeId)
		    VALUES (ttmd.TrackerMeasureName, ttmd.TrackerTypeId);

	IF (@DebugMode = 1)
	BEGIN
		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText) SELECT @DebugId SessionId, 'UpdateTracker' ApplicationName, SYSDATETIME() MessageDate, ' Merging into Tracker';
		SELECT @DebugId SessionId, 'UpdateTracker' ApplicationName, SYSDATETIME() MessageDate, ' Merging into Tracker';
	END;
	MERGE INTO Tracker AS T
	USING (SELECT @TrackerID TrackerId, @TrackerTypeId TrackerTypeId) AS tfd
		ON T.TrackerId = tfd.TrackerID
		AND T.TrackerTypeId = tfd.TrackerTypeId
	WHEN NOT MATCHED BY TARGET THEN
	   		INSERT (TrackerId, TrackerTypeId )
			VALUES (tfd.TrackerID, tfd.TrackerTypeId);
		
	IF (@DebugMode = 1)
	BEGIN
		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText) SELECT @DebugId SessionId, 'UpdateTracker' ApplicationName, SYSDATETIME() MessageDate, ' Merging into FilterGroups';
		SELECT @DebugId SessionId, 'UpdateTracker' ApplicationName, SYSDATETIME() MessageDate, ' Merging into FilterGroups';
	END;
	MERGE INTO FilterGroups AS FG
	USING (	SELECT ISNULL(@KeyFilterPassed,NULL) KeyFilter) AS FK
		ON ISNULL(FG.FilterGroupKeyFilter,'') = ISNULL(FK.KeyFilter,'')
		WHEN NOT MATCHED BY TARGET THEN
			INSERT (FilterGroupKeyFilter)
			VALUES (ISNULL(KeyFilter,NULL));

	IF (@DebugMode = 1)
	BEGIN
		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText) SELECT @DebugId SessionId, 'UpdateTracker' ApplicationName, SYSDATETIME() MessageDate, ' Merging into FilterGroupFilters';
		SELECT @DebugId SessionId, 'UpdateTracker' ApplicationName, SYSDATETIME() MessageDate, ' Merging into FilterGroupFilters';
		SELECT DISTINCT fg.FilterGroupID,ISNULL(@KeyFilterPassed,NULL) KeyFilter, 
						   CASE WHEN fl.Filter = '' THEN null
						   		WHEN fl.Filter is null THEN NULL
								ELSE Fl.Filter END Filter
				FROM FilterGroups fg, @FilterList FL
				WHERE @KeyFilterPassed = fg.FilterGroupKeyFilter
			UNION
			SELECT fg.FilterGroupID,ISNULL(@KeyFilterPassed,NULL) KeyFilter, ISNULL(@KeyFilterPassed,NULL) Filter
				FROM filterGroups FG
			WHERE ISNULL(@KeyFilterPassed,'') = ISNULL(fg.FilterGroupKeyFilter,'')
	END;
	MERGE INTO FilterGroupFilters AS FGF
	USING (SELECT DISTINCT fg.FilterGroupID,ISNULL(@KeyFilterPassed,NULL) KeyFilter, 
						   CASE WHEN fl.Filter = '' THEN null
						   		WHEN fl.Filter is null THEN NULL
								ELSE Fl.Filter END Filter
				FROM FilterGroups fg, @FilterList FL
				WHERE @KeyFilterPassed = fg.FilterGroupKeyFilter
			UNION
			SELECT fg.FilterGroupID,ISNULL(@KeyFilterPassed,NULL) KeyFilter, ISNULL(@KeyFilterPassed,NULL) Filter
				FROM filterGroups FG
			WHERE ISNULL(@KeyFilterPassed,'') = ISNULL(fg.FilterGroupKeyFilter,'')
			) AS FGFV 
		ON FGF.FilterGroupId = FGFV.FilterGroupID
		AND ISNULL(FGF.FilterGroupKeyFilter,'') = ISNULL(FGFV.KeyFilter,'')
		AND ISNULL(FGF.FilterGroupFilterString,'') = ISNULL(FGFV.Filter,'')
		WHEN NOT MATCHED BY TARGET THEN
			INSERT  (FilterGroupId, FilterGroupKeyFilter, FilterGroupFilterString)
			VALUES (FGFV.FilterGroupId, ISNULL(FGFV.KeyFilter,NULL), ISNULL(FGFV.Filter,NULL));

	IF (@DebugMode = 1)
	BEGIN
		INSERT INTO ProcessingLog (SessionId, ApplicationName,messageDate, MessageText) SELECT @DebugId SessionId, 'UpdateTracker' ApplicationName, SYSDATETIME() MessageDate, ' Merging into TrackerData';
		SELECT @DebugId SessionId, 'UpdateTracker' ApplicationName, SYSDATETIME() MessageDate, ' Merging into TrackerData';
		SELECT DISTINCT x.name,x.value, x.resolutionToWrite
	  					FROM @Measurements P7
				CROSS APPLY ( VALUES	(BUCKETRESOLUTION,NAME,VALUE,BUCKETRESOLUTION),
									(COVERSMINUTEBUCKET,NAME,VALUE,1),
									(COVERSFIVEMINUTEBUCKET,NAME,VALUE,5),
									(COVERSFIFTEENMINUTEBUCKET,NAME,VALUE,15),
									(COVERSTHIRTYMINUTEBUCKET,NAME,VALUE,30),
									(COVERSHOURBUCKET,NAME,VALUE,60),
									(COVERSDAYBUCKET,NAME,VALUE,1440),
									(COVERSMONTHBUCKET,NAME,VALUE,43200),
									(@MinResolution,NAME, VALUE, @MinResolution)
							) X (BITVALUE,NAME, VALUE, RESOLUTIONTOWRITE)
					WHERE X.BITVALUE > 0
		SELECT m.*, f.FilterGroupID, @TimeSlot TimeSlotDt, @TrackerId TrackerId, ISNULL(@KeyFilterPassed,NULL) KeyFilter, @Trackertypeid TrackerTypeId, tm.TrackerMeasureid
				FROM (SELECT DISTINCT x.name,x.value, x.resolutionToWrite
	  					FROM @Measurements P7
				CROSS APPLY ( VALUES	(BUCKETRESOLUTION,NAME,VALUE,BUCKETRESOLUTION),
									(COVERSMINUTEBUCKET,NAME,VALUE,1),
									(COVERSFIVEMINUTEBUCKET,NAME,VALUE,5),
									(COVERSFIFTEENMINUTEBUCKET,NAME,VALUE,15),
									(COVERSTHIRTYMINUTEBUCKET,NAME,VALUE,30),
									(COVERSHOURBUCKET,NAME,VALUE,60),
									(COVERSDAYBUCKET,NAME,VALUE,1440),
									(COVERSMONTHBUCKET,NAME,VALUE,43200),
									(@MinResolution,NAME, VALUE, @MinResolution)
							) X (BITVALUE,NAME, VALUE, RESOLUTIONTOWRITE)
					WHERE X.BITVALUE > 0
					) M, TrackerMeasures tm,
					FilterGroups f
					WHERE ISNULL(@KeyFilterPassed,'') = ISNULL(f.FilterGroupKeyFilter,'')
				AND @TrackerTypeId = tm.TrackerTypeId
				and m.Name = tm.TrackerMeasureName
					
	END;
	MERGE INTO TrackerData AS TD
	USING (SELECT m.*, f.FilterGroupID, @TimeSlot TimeSlotDt, @TrackerId TrackerId, ISNULL(@KeyFilterPassed,NULL) KeyFilter, @Trackertypeid TrackerTypeId, tm.TrackerMeasureid
				FROM (SELECT DISTINCT x.name,x.value, x.resolutionToWrite
	  					FROM @Measurements P7
				CROSS APPLY ( VALUES	(BUCKETRESOLUTION,NAME,VALUE,BUCKETRESOLUTION),
									(COVERSMINUTEBUCKET,NAME,VALUE,1),
									(COVERSFIVEMINUTEBUCKET,NAME,VALUE,5),
									(COVERSFIFTEENMINUTEBUCKET,NAME,VALUE,15),
									(COVERSTHIRTYMINUTEBUCKET,NAME,VALUE,30),
									(COVERSHOURBUCKET,NAME,VALUE,60),
									(COVERSDAYBUCKET,NAME,VALUE,1440),
									(COVERSMONTHBUCKET,NAME,VALUE,43200),
									(@MinResolution,NAME, VALUE, @MinResolution)
							) X (BITVALUE,NAME, VALUE, RESOLUTIONTOWRITE)
					WHERE X.BITVALUE > 0
					) M, TrackerMeasures tm,
					FilterGroups f
			WHERE ISNULL(@KeyFilterPassed,'') = ISNULL(f.FilterGroupKeyFilter,'')
				AND @TrackerTypeId = tm.TrackerTypeId
				and m.Name = tm.TrackerMeasureName
			) AS DMGM
		ON TD.TimeSlotDt = DMGM.TimeSlotDt
		AND TD.TrackerId = DMGM.TrackerId
		AND TD.TrackerTypeId = DMGM.TrackerTypeId
		AND ISNULL(TD.KeyFilter,'') = ISNULL(DMGM.KeyFilter,'')
		AND TD.TrackerMeasureId = DMGM.TrackerMeasureid
		AND TD.FilterGroupId = DMGM.FilterGroupId
		AND TD.DataResolution = DMGM.ResolutionToWrite
		WHEN NOT MATCHED BY TARGET THEN
	 		INSERT (TrackerId,TrackerTypeId, TimeSlotDt, TimeSlotStr,KeyFilter,TrackerMeasureid, MeasurementValue, FilterGroupID, DataResolution)
			VALUES (DMGM.TrackerId,DMGM.TrackerTypeId, DMGM.TimeSlotDt,CONVERT(NVARCHAR(25),DMGM.TimeSlotDt,120), ISNULL(DMGM.KeyFilter,NULL),DMGM.TrackerMeasureId,DMGM.Value, DMGM.FilterGroupId, DMGM.ResolutionToWrite)
		WHEN MATCHED THEN
			UPDATE SET TD.MeasurementValue = TD.MeasurementValue + DMGM.Value;

	SELECT 
		@error = @@ERROR

	IF @error <> 0 
	BEGIN
		ROLLBACK TRANSACTION
		GOTO ERR_EXIT
	END

	OK_EXIT: 
		COMMIT TRANSACTION
		IF @return IS NULL SELECT @return = @RET_OK
		SET NOCOUNT OFF
		RETURN @return
	
	ERR_EXIT:
		IF (@return IS NULL OR @return = 0) SELECT @return = @error
		RETURN @return
END



GO
