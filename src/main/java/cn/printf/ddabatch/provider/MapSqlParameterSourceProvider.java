package cn.printf.ddabatch.provider;

import org.springframework.batch.item.database.ItemSqlParameterSourceProvider;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.util.Map;

public class MapSqlParameterSourceProvider implements ItemSqlParameterSourceProvider<Map> {
    @Override
    public SqlParameterSource createSqlParameterSource(Map item) {
        return new MapSqlParameterSource(item);
    }
}
