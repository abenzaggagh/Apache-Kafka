package com.sha.springbootkafka.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * @author sa
 * @date 14.11.2021
 * @time 13:52
 */
@Data
@NoArgsConstructor // public MessageEntity();
@AllArgsConstructor // public MessageEntity(String type, LocalDateTime time);
public class MessageEntity
{
    private String type;
    private LocalDateTime time;
}
