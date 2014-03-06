/*
 *    _____      _ ____  ____
 *   / ___/_____(_) __ \/ __ )
 *   \__ \/ ___/ / / / / __  |
 *  ___/ / /__/ / /_/ / /_/ / 
 * /____/\___/_/_____/_____/  
 *
 *
 * BEGIN_COPYRIGHT
 *
 * This file is part of SciDB.
 * Copyright (C) 2008-2014 SciDB, Inc.
 *
 * SciDB is free software: you can redistribute it and/or modify
 * it under the terms of the AFFERO GNU General Public License as published by
 * the Free Software Foundation.
 *
 * SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
 * INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
 * NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
 * the AFFERO GNU General Public License for the complete license terms.
 *
 * You should have received a copy of the AFFERO GNU General Public License
 * along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
 *
 * END_COPYRIGHT
 */
#include "query/Operator.h"

namespace scidb
{

class LogicalCollate : public LogicalOperator
{
public:
    LogicalCollate(const string& logicalName, const string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT()
    }

// Relax this to simply check that they are all of the same type
    size_t checkInputAttributes(ArrayDesc const& inputSchema)
    {
        Attributes const& attrs = inputSchema.getAttributes(true);
        size_t const nAttrs = attrs.size();
        for (AttributeID i =0; i<nAttrs; ++i)
        {
            if (attrs[i].getType() != TID_DOUBLE)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_ILLEGAL_OPERATION)
                      << "collate only accepts an input with attributes of type double";
            }
        }
        return (size_t) nAttrs;
    }

    ArrayDesc inferSchema(vector< ArrayDesc> schemas, shared_ptr< Query> query)
    {
        ArrayDesc const& inputSchema = schemas[0];
        size_t nAttrs = checkInputAttributes(inputSchema);
        Attributes outputAttributes;
// XXX loosen up double value requirement
        outputAttributes.push_back(AttributeDesc(0, "val", TID_DOUBLE, 0, 0));
        outputAttributes = addEmptyTagAttribute(outputAttributes);
        Dimensions outputDimensions;
        outputDimensions.push_back(DimensionDesc("i", 0, inputSchema.getDimensions()[0].getEndMax(), inputSchema.getDimensions()[0].getChunkInterval(), 0));
        outputDimensions.push_back(DimensionDesc("j", 0, nAttrs, nAttrs, 0));
        return ArrayDesc(inputSchema.getName(), outputAttributes, outputDimensions);

    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalCollate, "collate");

}
