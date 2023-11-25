#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsConversion.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>


namespace DB
{
namespace
{
    class FunctionHelloWorld : public IFunction
    {
    public:
        static constexpr auto name = "helloWorldWithStringColumn";

        static FunctionPtr create(ContextPtr)
        {
            return std::make_shared<FunctionHelloWorld>();
        }

        std::string getName() const override
        {
            return name;
        }

        size_t getNumberOfArguments() const override { return 1; }
        bool useDefaultImplementationForConstants() const override { return true; }
        bool useDefaultImplementationForNulls() const override { return false; }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
        {
            auto string_type = DataTypeFactory::instance().get("String");
            return arguments[0]->isNullable() ? makeNullable(string_type) : string_type;
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
        {  
            if (const ColumnString * col_from = checkAndGetColumn<ColumnString>(arguments[0].column.get()))
            {
                const typename ColumnString::Chars & source_data = col_from->getChars();

                const char* prefix_str = "helloWorld";
                // size_t target_chars_size = sizeof(prefix_str) + source_data.size();
                
                // auto offsets_col = ColumnString::ColumnOffsets::create();
                // auto & offsets_data = offsets_col->getData();
                // offsets_data.resize(input_rows_count);

                auto data_col = ColumnString::create();
                // auto & res_data = data_col->getChars();
                // res_data.resize(target_chars_size);

                // memcpy(&res_data[0], prefix_str, sizeof(prefix_str));
                // memcpy(&res_data[sizeof(prefix_str)-1], &source_data[0], source_data.size());

                // ColumnString::Offset current_offset = 0;
                for (size_t i = 0; i < input_rows_count; ++i) {
                    StringRef current_row = col_from->getDataAt(i);
                    auto current_row_data = current_row.toString();
                    // Concatenate the strings
                    std::string concatenated = prefix_str + current_row_data;
                    // Convert the resulting std::string to a char*
                    char* concatenatedCharPtr = new char[concatenated.size() + 1];
                    std::strcpy(concatenatedCharPtr, concatenated.c_str());
                    data_col->insertData(concatenatedCharPtr, sizeof(concatenatedCharPtr) + sizeof(prefix_str));
                }
                
                return data_col;
            }
            else
                throw Exception("Illegal column " + arguments[0].column->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
        }
    };

}

REGISTER_FUNCTION(HelloWorld)
{
    factory.registerFunction<FunctionHelloWorld>();
}

}
