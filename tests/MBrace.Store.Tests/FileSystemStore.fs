﻿namespace Nessos.MBrace.Store.Tests.FileSystem

open NUnit.Framework
open FsUnit

open Nessos.MBrace.Store.Tests

[<TestFixture>]
type ``FileSystem File store tests`` () =
    inherit  ``File Store Tests``(StoreConfiguration.fileSystemStore)

[<TestFixture>]
type ``FileSystem Table store tests`` () =
    inherit  ``Table Store Tests``(StoreConfiguration.fileSystemStore)