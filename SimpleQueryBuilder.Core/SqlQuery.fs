namespace rec SimpleQueryBuilder.Core

open System
open SimpleQueryBuilder.Core.Util

type SqlQuery =
    {
        With: string seq
        Select: string seq
        From: string seq
        Joins: string seq
        Where: string seq
        Sorting: SortingCriteria seq
        GroupBy: string seq
        Pagination: PaginationCriteria option
        Mappings: (SqlQuery -> SqlQuery) seq
        Translator: QueryTranslator
    }

and PaginationCriteria =
    {
        /// Pages should be 0-indexed
        Page: int
        Size: int
    }

and QueryTranslator =
    {
        withCte: SqlQuery -> string
        select: SqlQuery -> string
        from: SqlQuery -> string
        joins: SqlQuery -> string
        where: SqlQuery -> string
        orderBy: SqlQuery -> string
        groupBy: SqlQuery -> string
        limit: SqlQuery -> string
        count: string -> string
        includeCount: string -> string
        pagination: PaginationCriteria -> string
    }

[<RequireQualifiedAccess>]
module SqlQuery =
    let private unspecifiedTranslator = fun _ -> failwith "Query translator not specified"

    let private defaultTranslator =
        {
            withCte = unspecifiedTranslator
            select = unspecifiedTranslator
            from = unspecifiedTranslator
            joins = unspecifiedTranslator
            where = unspecifiedTranslator
            orderBy = unspecifiedTranslator
            groupBy = unspecifiedTranslator
            limit = unspecifiedTranslator
            count = unspecifiedTranslator
            includeCount = unspecifiedTranslator
            pagination = unspecifiedTranslator
        }

    let empty =
        {
            With = Seq.empty
            Select = Seq.empty
            From = Seq.empty
            Joins = Seq.empty
            Where = Seq.empty
            Sorting = Seq.empty
            GroupBy = Seq.empty
            Pagination = None
            Mappings = Seq.empty
            Translator = defaultTranslator
        }

    let toString (query: SqlQuery) =
        let query = query.Mappings |> Seq.fold (fun query f -> f query) query

        [
            query.Translator.withCte query
            query.Translator.select query
            query.Translator.from query
            query.Translator.joins query
            query.Translator.where query
            query.Translator.groupBy query
            query.Translator.orderBy query
            query.Translator.limit query
        ]
        |> List.filter (not << String.IsNullOrWhiteSpace)
        |> String.concat Environment.NewLine
        |> String.trim
        |> fun query -> query + ";"

    let withCte withCte query =
        { query with With = Seq.concat [ query.With; withCte ] }

    let select select (query: SqlQuery) =
        { query with Select = Seq.concat [ query.Select; select ] }

    let where filters (query: SqlQuery) =
        { query with Where = Seq.concat [ query.Where; filters ] }

    let from tables (query: SqlQuery) =
        { query with From = Seq.concat [ query.From; tables ] }

    let joins joinExpressions (query: SqlQuery) =
        { query with
            Joins =
                Seq.concat [ query.Joins
                             joinExpressions ]
        }

    let whereIf condition filters query =
        if condition then query |> where filters else query

    let whereSome opt v query =
        match opt with
        | None -> query
        | Some _ -> { query with Where = Seq.append [ v ] query.Where }

    let whereNone opt v query =
        match opt with
        | None -> { query with Where = Seq.append [ v ] query.Where }
        | Some _ -> query

    /// Overwrites any columns previously specified.
    let count criteria query =
        { query with Select = query.Translator.count criteria |> Seq.singleton }

    /// Includes the total count of all rows matching the query (regardless of paging) as an additional column
    let includeCount criteria query =
        query |> select [ query.Translator.includeCount criteria ]

    let includeTotalCount = includeCount "*"

    let countAll query =
        query |> count "*"

    let countAllIf isCount query =
        match isCount with
        | false -> query
        | true -> countAll query

    let sorting sorting query =
        { query with Sorting = Seq.concat [ query.Sorting; sorting ] }

    let groupBy groupBy query =
        { query with GroupBy = Seq.concat [ query.GroupBy; groupBy ] }

    let sortBy (sortingCriteria: SortingCriteria list option) (query: SqlQuery) =
        match sortingCriteria with
        | None -> query
        | Some ordering -> query |> sorting ordering

    let sortByColumn (sortingCriteria: SortingCriteria option) (query: SqlQuery) =
        match sortingCriteria with
        | None -> query
        | Some ordering -> query |> sorting [ ordering ]

    let paginateOption pagination query =
        { query with Pagination = pagination }

    let paginate p =
        Some p |> paginateOption

    /// Merge two queries together. Elements in `a` will go before `b`. Will use the Translator for `a`.
    let extend (a: SqlQuery) b =
        {
            With = Seq.concat [ a.With; b.With ]
            Select = Seq.concat [ a.Select; b.Select ]
            From = Seq.concat [ a.From; b.From ]
            Joins = Seq.concat [ a.Joins; b.Joins ]
            Where = Seq.concat [ a.Where; b.Where ]
            Sorting = Seq.concat [ a.Sorting; b.Sorting ]
            GroupBy = Seq.concat [ a.GroupBy; b.GroupBy ]
            Pagination = Option.orElse a.Pagination b.Pagination
            Mappings = Seq.concat [ a.Mappings; b.Mappings ]
            Translator = a.Translator
        }

    let map f (query: SqlQuery) : SqlQuery =
        { query with
            Mappings =
                seq {
                    yield! query.Mappings
                    yield f
                }
        }

type SqlQueryBuilder (translator: QueryTranslator) =
    member _.Yield _ =
        { SqlQuery.empty with Translator = translator }

    [<CustomOperation("withCte")>]
    member _.With (query: SqlQuery, [<ParamArray>] withCte: string []) =
        query |> SqlQuery.withCte withCte

    [<CustomOperation("select")>]
    member this.Select (query: SqlQuery, select: string seq) =
        query |> SqlQuery.select select

    [<CustomOperation("select")>]
    member _.Select (query: SqlQuery, [<ParamArray>] select: string []) =
        query |> SqlQuery.select select

    [<CustomOperation("where")>]
    member _.Where (query: SqlQuery, [<ParamArray>] filters: string []) =
        query |> SqlQuery.where filters

    [<CustomOperation("where")>]
    member _.Where (query: SqlQuery, filters: string seq) =
        query |> SqlQuery.where filters

    [<CustomOperation("where")>]
    member _.Where (query: SqlQuery, condition: bool, [<ParamArray>] filters: string []) =
        query |> SqlQuery.whereIf condition filters

    [<CustomOperation("whereSome")>]
    member _.WhereSome (query: SqlQuery, opt, v) =
        query |> SqlQuery.whereSome opt v

    [<CustomOperation("whereNone")>]
    member _.WhereNone (query: SqlQuery, opt, v) =
        query |> SqlQuery.whereNone opt v

    [<CustomOperation("whereNot")>]
    member _.WhereFalse (query: SqlQuery, condition: bool, filter) =
        query |> SqlQuery.whereIf (not condition) filter

    [<CustomOperation("from")>]
    member _.From (query: SqlQuery, [<ParamArray>] from: string []) =
        query |> SqlQuery.from from

    [<CustomOperation("from")>]
    member _.From (query: SqlQuery, from: string seq) =
        query |> SqlQuery.from from

    /// Omit the join keyword from the given string, it is prepended automatically for readability.
    [<CustomOperation("innerJoin")>]
    member _.InnerJoin (query: SqlQuery, join) =
        query |> SqlQuery.joins [ sprintf "inner join %s" join ]

    /// Omit the join keyword from the given string, it is prepended automatically for readability.
    [<CustomOperation("leftJoin")>]
    member _.LeftJoin (query: SqlQuery, join) =
        query |> SqlQuery.joins [ sprintf "left join %s" join ]

    [<CustomOperation("countAllIf")>]
    member _.CountAllIf (query: SqlQuery, isCount) =
        query |> SqlQuery.countAllIf isCount

    [<CustomOperation("countAll")>]
    member _.CountAll (query) =
        query |> SqlQuery.countAll

    [<CustomOperation("count")>]
    member _.Count (query, criteria) =
        query |> SqlQuery.count criteria

    [<CustomOperation("includeCount")>]
    member _.IncludeCount (query, criteria) =
        query |> SqlQuery.includeCount criteria

    [<CustomOperation("includeTotalCount")>]
    member _.IncludeTotalCount (query) =
        query |> SqlQuery.includeTotalCount

    [<CustomOperation("groupBy")>]
    member _.GroupBy (query: SqlQuery, [<ParamArray>] groupBy: string []) =
        query |> SqlQuery.groupBy groupBy

    [<CustomOperation("orderBy")>]
    member _.OrderBy (query: SqlQuery, [<ParamArray>] sorting: SortingCriteria []) =
        query |> SqlQuery.sorting sorting

    [<CustomOperation("orderBy")>]
    member _.OrderBy (query: SqlQuery, sorting: SortingCriteria option) =
        query |> SqlQuery.sortByColumn sorting

    /// Order by `field`, ascending
    [<CustomOperation("orderBy")>]
    member _.OrderBy (query: SqlQuery, field) =
        query |> SqlQuery.sorting [ Ascending field ]

    /// Order by `field`, ascending
    [<CustomOperation("orderBy")>]
    member _.OrderBy (query: SqlQuery, field) =
        match field with
        | None -> query
        | Some field -> query |> SqlQuery.sorting [ Ascending field ]

    /// Order by `field`, ascending
    [<CustomOperation("orderBy")>]
    member _.OrderByFields (query: SqlQuery, fields) =
        query |> SqlQuery.sorting (fields |> Seq.map Ascending)

    [<CustomOperation("orderByDescending")>]
    member _.OrderByDescending (query: SqlQuery, field) =
        query |> SqlQuery.sorting [ Descending field ]

    [<CustomOperation("orderByDescending")>]
    member _.OrderByDescending (query: SqlQuery, field) =
        match field with
        | None -> query
        | Some field -> query |> SqlQuery.sorting [ Descending field ]

    [<CustomOperation("orderByDescending")>]
    member _.OrderByFieldsDescending (query: SqlQuery, fields) =
        query |> SqlQuery.sorting (fields |> Seq.map Ascending)

    [<CustomOperation("paginate")>]
    member _.Paginate (query: SqlQuery, pagination: PaginationCriteria) =
        query |> SqlQuery.paginate pagination

    [<CustomOperation("paginate")>]
    member _.Paginate (query: SqlQuery, pagination: PaginationCriteria option) =
        query |> SqlQuery.paginateOption pagination

    [<CustomOperation("extend")>]
    member this.Extend (query: SqlQuery, other: SqlQuery) =
        other |> SqlQuery.extend query

    [<CustomOperation("map")>]
    member _.Map (query: SqlQuery, f) : SqlQuery =
        query |> SqlQuery.map f

type SortingCriteria =
    | Ascending of string
    | Descending of string
    override this.ToString () =
        match this with
        | Ascending field -> $"{field} asc"
        | Descending field -> $"{field} desc"

module PaginationCriteria =

    [<Literal>]
    let MinimumSize = 1

    let truncate maximumSize size =
        min size maximumSize |> max MinimumSize

    let create maximumSize page size =
        { Page = max page 0; Size = truncate maximumSize size }

[<AutoOpen>]
module SqlQueryBuilder =
    let sqlQuery translator =
        SqlQueryBuilder translator
