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
        ADD_PARAM_SCHEMA()
    }

// Relax this to simply check that they are all of the same type
// But add checks on chunk sizes (all output columns in one chunk)
    void checkInputAttributes(ArrayDesc const& inputSchema)
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
    }

    ArrayDesc inferSchema(vector< ArrayDesc> schemas, shared_ptr< Query> query)
    {
        assert(schemas.size() == 1);
        assert(_parameters.size() == 1);

        ArrayDesc outputArray = ((boost::shared_ptr<OperatorParamSchema>&) _parameters[0])->getSchema();

        Attributes const& outputAttributes = outputArray.getAttributes();
        Dimensions const& outputDimensions = outputArray.getDimensions();

        return ArrayDesc(outputArray.getName(), outputAttributes, outputDimensions);
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalCollate, "collate");

}
